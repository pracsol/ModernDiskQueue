// Copyright (c) 2005 - 2008 Ayende Rahien (ayende@ayende.com)
// Extensions (c) 2008-2022 Iain Ballard
// All rights reserved.
// 
// Redistribution and use in source and binary forms, with or without modification,
// are permitted provided that the following conditions are met:
// 
//     * Redistributions of source code must retain the above copyright notice,
//     this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//     * Neither the name of Ayende Rahien nor the name of the product nor the names
//     of its contributors may be used to endorse or promote products derived from
//     this software without specific prior written permission.
// 
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
// THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace DiskQueue.Implementation
{
	internal class PersistentQueueImpl : IPersistentQueueImpl
	{
	    private readonly HashSet<Entry> _checkedOutEntries = new();

		private readonly Dictionary<int, int> _countOfItemsPerFile = new();

		private readonly LinkedList<Entry> _entries = new();

		private readonly string _path;

		private readonly object _transactionLogLock = new();
		private readonly object _writerLock = new();
	    private readonly bool _throwOnConflict;
	    private static readonly object _configLock = new();
	    private volatile bool _disposed;
		private FileStream? _fileLock;

		public int SuggestedReadBuffer { get; set; }
		public int SuggestedWriteBuffer { get; set; }
		public long SuggestedMaxTransactionLogSize { get; set; }
		
		/// <summary>
		/// <para>Setting this to false may cause unexpected data loss in some failure conditions.</para>
		/// <para>Defaults to true.</para>
		/// <para>If true, each transaction commit will flush the transaction log.</para>
		/// <para>This is slow, but ensures the log is correct per transaction in the event of a hard termination (i.e. power failure)</para>
		/// </summary>
		public bool ParanoidFlushing { get; set; }
		public bool AllowTruncatedEntries { get; set; }
		public bool TrimTransactionLogOnDispose { get; set; }

	    public readonly int MaxFileSize;

		public PersistentQueueImpl(string path, int maxFileSize, bool throwOnConflict)
		{
		    lock(_configLock)
			{
				_disposed = true;
				TrimTransactionLogOnDispose = PersistentQueue.DefaultSettings.TrimTransactionLogOnDispose;
				ParanoidFlushing = PersistentQueue.DefaultSettings.ParanoidFlushing;
				AllowTruncatedEntries = PersistentQueue.DefaultSettings.AllowTruncatedEntries;
				SuggestedMaxTransactionLogSize = Constants._32Megabytes;
				SuggestedReadBuffer = 1024*1024;
				SuggestedWriteBuffer = 1024*1024;
                this._throwOnConflict = throwOnConflict;

                MaxFileSize = maxFileSize;
				try
				{
					this._path = Path.GetFullPath(path);
					if (!FileOperations.DirectoryExists(this._path))
						CreateDirectory(this._path);

					LockQueue();
				}
				catch (UnauthorizedAccessException)
				{
					throw new UnauthorizedAccessException("Directory \"" + path + "\" does not exist or is missing write permissions");
				}
				catch (IOException e)
				{
					GC.SuppressFinalize(this); //avoid finalizing invalid instance
					throw new InvalidOperationException("Another instance of the queue is already in action, or directory does not exists", e);
				}

				try
				{
					ReadMetaState();
					ReadTransactionLog();
				}
				catch (Exception)
				{
					GC.SuppressFinalize(this); //avoid finalizing invalid instance
					UnlockQueue();
					throw;
				}
				_disposed = false;
			}
		}

		void UnlockQueue()
		{
			lock (_writerLock)
			{
				if (string.IsNullOrWhiteSpace(_path)) return;
				var target = Path.Combine(_path, "lock");
				if (_fileLock != null)
				{
					_fileLock.Dispose();
					FileOperations.PrepareDelete(target);
				}

				_fileLock = null;
			}
			FileOperations.Finalise();
		}

		void LockQueue()
		{
			lock (_writerLock)
			{
				var target = Path.Combine(_path, "lock");
				_fileLock = FileOperations.CreateNoShareFile(target);
			}
		}

		void CreateDirectory(string s)
		{
			FileOperations.CreateDirectory(s);
			SetPermissions.TryAllowReadWriteForAll(s);
		}

		// ReSharper disable once IntroduceOptionalParameters.Global
		public PersistentQueueImpl(string path) : this(path, Constants._32Megabytes, true) { }

		public int EstimatedCountOfItemsInQueue
		{
			get
			{
				lock (_entries)
				{
					return _entries.Count + _checkedOutEntries.Count;
				}
			}
		}


		private int CurrentCountOfItemsInQueue
		{
			get
			{
				lock (_entries)
				{
					return _entries.Count + _checkedOutEntries.Count;
				}
			}
		}

		public long CurrentFilePosition { get; private set; }

		private string TransactionLog => Path.Combine(_path, "transaction.log");

		private string Meta => Path.Combine(_path, "meta.state");

		public int CurrentFileNumber { get; private set; }

		~PersistentQueueImpl()
		{
			if (_disposed) return;
			Dispose();
		}

		public void Dispose()
		{
			lock (_configLock)
			{
				if (_disposed) return;
				try
				{
					_disposed = true;
					lock (_transactionLogLock)
					{
						if (TrimTransactionLogOnDispose) FlushTrimmedTransactionLog();
					}
					GC.SuppressFinalize(this);
				}
				finally
				{
					UnlockQueue();
				}
			}
		}

		public void AcquireWriter(Stream stream, Func<Stream, long> action, Action<Stream> onReplaceStream)
		{
			lock (_writerLock)
			{
				if (stream.Position != CurrentFilePosition)
				{
					stream.Position = CurrentFilePosition;
				}
				CurrentFilePosition = action(stream);
				if (CurrentFilePosition < MaxFileSize) return;

				CurrentFileNumber += 1;
				var writer = CreateWriter();
				// we assume same size messages, or near size messages
				// that gives us a good heuristic for creating the size of 
				// the new file, so it wouldn't be fragmented
				writer.SetLength(CurrentFilePosition);
				CurrentFilePosition = 0;
				onReplaceStream(writer);
			}
		}

		public void CommitTransaction(ICollection<Operation> operations)
		{
			if (operations.Count == 0)
				return;

			byte[] transactionBuffer = GenerateTransactionBuffer(operations);

			lock (_transactionLogLock)
			{
				long txLogSize;
				using ( var stream = WaitForTransactionLog(transactionBuffer))
				{
					stream.Write(transactionBuffer, 0, transactionBuffer.Length);
					txLogSize = stream.Position;
					stream.HardFlush();
				}

				ApplyTransactionOperations(operations);
				TrimTransactionLogIfNeeded(txLogSize);

				Atomic.Write(Meta, stream =>
				{
					var bytes = BitConverter.GetBytes(CurrentFileNumber);
					stream.Write(bytes, 0, bytes.Length);
					bytes = BitConverter.GetBytes(CurrentFilePosition);
					stream.Write(bytes, 0, bytes.Length);
				});

				if (ParanoidFlushing) FlushTrimmedTransactionLog();
			}
		}

		FileStream WaitForTransactionLog(byte[] transactionBuffer)
		{
			for (int i = 0; i < 10; i++)
			{
				try
				{
					return new FileStream(TransactionLog,
										  FileMode.Append,
										  FileAccess.Write,
										  FileShare.None,
										  transactionBuffer.Length,
										  FileOptions.SequentialScan | FileOptions.WriteThrough);
				}
				catch (Exception)
				{
					Thread.Sleep(250);
				}
			}
			throw new TimeoutException("Could not acquire transaction log lock");
		}

		public Entry? Dequeue()
		{
			lock (_entries)
			{
				var first = _entries.First;
				if (first == null) return null;
				var entry = first.Value;
				if (entry == null) throw new Exception("Entry queue was in an invalid state: null entry");
				
				if (entry.Data == null)
				{
					var ok = ReadAhead();
					if (!ok) return null;
				}
				_entries.RemoveFirst();
				// we need to create a copy so we will not hold the data
				// in memory as well as the position
			    lock (_checkedOutEntries)
			    {
			        _checkedOutEntries.Add(new Entry(entry.FileNumber, entry.Start, entry.Length));
			    }
			    return entry;
			}
		}

		/// <summary>
		/// Assumes that entries has at least one entry. Should be called inside a lock.
		/// </summary>
		private bool ReadAhead()
		{
			long currentBufferSize = 0;
			
			var firstEntry = _entries.First?.Value ?? throw new Exception("Invalid queue state: first entry is null");
			Entry lastEntry = firstEntry;
			foreach (var entry in _entries)
			{
				// we can't read ahead to another file or
				// if we have unordered queue, or sparse items
				if (entry != lastEntry &&
					(entry.FileNumber != lastEntry.FileNumber ||
					entry.Start != (lastEntry.Start + lastEntry.Length)))
					break;
				if (currentBufferSize + entry.Length > SuggestedReadBuffer)
					break;
				lastEntry = entry;
				currentBufferSize += entry.Length;
			}
			if (lastEntry == firstEntry)
				currentBufferSize = lastEntry.Length;

			var buffer = ReadEntriesFromFile(firstEntry, currentBufferSize);
			if (buffer.IsFailure)
			{
				if (AllowTruncatedEntries) return false;
				throw buffer.Error!;
			}

			var index = 0;
			foreach (var entry in _entries)
			{
				entry.Data = new byte[entry.Length];
				Buffer.BlockCopy(buffer.Value!, index, entry.Data, 0, entry.Length);
				index += entry.Length;
				if (entry == lastEntry)
					break;
			}
			return true;
		}

		private Maybe<byte[]> ReadEntriesFromFile(Entry firstEntry, long currentBufferSize)
		{
			try
			{
				var buffer = new byte[currentBufferSize];
				if (firstEntry.Length < 1) return buffer.Success();
				using var reader = new FileStream(GetDataPath(firstEntry.FileNumber),
					FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite);
				reader.Position = firstEntry.Start;
				var totalRead = 0;
				do
				{
					var bytesRead = reader.Read(buffer, totalRead, buffer.Length - totalRead);
					if (bytesRead == 0)
					{
						return Maybe<byte[]>.Fail(new InvalidOperationException("End of file reached while trying to read queue item"));
					}

					totalRead += bytesRead;
				} while (totalRead < buffer.Length);

				return buffer.Success();
			}
			catch (Exception ex)
			{
				return Maybe<byte[]>.Fail(ex);
			}
		}

		public IPersistentQueueSession OpenSession()
		{
			return new PersistentQueueSession(this, CreateWriter(), SuggestedWriteBuffer);
		}

		public void Reinstate(IEnumerable<Operation> reinstatedOperations)
		{
			lock (_entries)
			{
				ApplyTransactionOperations(
					from entry in reinstatedOperations.Reverse()
					where entry.Type == OperationType.Dequeue
					select new Operation(
						OperationType.Reinstate,
						entry.FileNumber,
						entry.Start,
						entry.Length
						)
					);
			}
		}

		private void ReadTransactionLog()
		{
			var requireTxLogTrimming = false;
			Atomic.Read(TransactionLog, stream =>
			{
				using (var binaryReader = new BinaryReader(stream))
				{
					bool readingTransaction = false;
					try
					{
						int txCount = 0;
						while (true)
						{
                            txCount += 1;
							// this code ensures that we read the full transaction
							// before we start to apply it. The last truncated transaction will be
							// ignored automatically.
							AssertTransactionSeparator(binaryReader, txCount, Marker.StartTransaction,
													   () => readingTransaction = true);

							var opsCount = binaryReader.ReadInt32();
							var txOps = new List<Operation>(opsCount);
							for (var i = 0; i < opsCount; i++)
							{
								AssertOperationSeparator(binaryReader);
								var operation = new Operation(
									(OperationType)binaryReader.ReadByte(),
									binaryReader.ReadInt32(),
									binaryReader.ReadInt32(),
									binaryReader.ReadInt32()
								);
								txOps.Add(operation);
								//if we have non enqueue entries, this means 
								// that we have not closed properly, so we need
								// to trim the log
								if (operation.Type != OperationType.Enqueue)
									requireTxLogTrimming = true;
							}
                            // check that the end marker is in place
							AssertTransactionSeparator(binaryReader, txCount, Marker.EndTransaction, () => { });
							readingTransaction = false;
							ApplyTransactionOperations(txOps);
						}
					}
					catch (EndOfStreamException)
					{
						// we have a truncated transaction, need to clear that
						if (readingTransaction) requireTxLogTrimming = true;
					}
				}
			});
			if (requireTxLogTrimming) FlushTrimmedTransactionLog();
		}

		private void FlushTrimmedTransactionLog()
		{
			byte[] transactionBuffer;
			using (var ms = new MemoryStream())
			{
				ms.Write(Constants.StartTransactionSeparator, 0, Constants.StartTransactionSeparator.Length);

				var count = BitConverter.GetBytes(EstimatedCountOfItemsInQueue);
				ms.Write(count, 0, count.Length);
                
			    Entry[] checkedOut;
			    lock (_checkedOutEntries)
			    {
			        checkedOut = _checkedOutEntries.ToArray();
			    }
			    foreach (var entry in checkedOut)
				{
					WriteEntryToTransactionLog(ms, entry, OperationType.Enqueue);
				}

			    Entry[] listedEntries;
                lock (_entries)
                {
                    listedEntries = ToArray(_entries);
			    }

			    foreach (var entry in listedEntries)
				{
					WriteEntryToTransactionLog(ms, entry, OperationType.Enqueue);
				}
				ms.Write(Constants.EndTransactionSeparator, 0, Constants.EndTransactionSeparator.Length);
				ms.HardFlush();
				transactionBuffer = ms.ToArray();
			}
			Atomic.Write(TransactionLog, stream =>
			{
				stream.SetLength(transactionBuffer.Length);
				stream.Write(transactionBuffer, 0, transactionBuffer.Length);
			});
		}

        /// <summary>
        /// This special purpose function is to work around potential issues with Mono
        /// </summary>
	    private static Entry[] ToArray(LinkedList<Entry>? list)
        {
            if (list == null) return Array.Empty<Entry>();
            var outp = new List<Entry>(25);
            var cur = list.First;
            while (cur != null)
            {
                outp.Add(cur.Value);
                cur = cur.Next;
            }
            return outp.ToArray();
        }

	    private static void WriteEntryToTransactionLog(Stream ms, Entry entry, OperationType operationType)
		{
			ms.Write(Constants.OperationSeparatorBytes, 0, Constants.OperationSeparatorBytes.Length);

			ms.WriteByte((byte)operationType);

			var fileNumber = BitConverter.GetBytes(entry.FileNumber);
			ms.Write(fileNumber, 0, fileNumber.Length);

			var start = BitConverter.GetBytes(entry.Start);
			ms.Write(start, 0, start.Length);

			var length = BitConverter.GetBytes(entry.Length);
			ms.Write(length, 0, length.Length);
		}

		private void AssertOperationSeparator(BinaryReader reader)
		{
			var separator = reader.ReadInt32();
		    if (separator == Constants.OperationSeparator) return; // OK

            ThrowIfStrict("Unexpected data in transaction log. Expected to get transaction separator but got unknown data");
        }

        public int[] ApplyTransactionOperationsInMemory(IEnumerable<Operation>? operations)
		{
			if (operations == null) return Array.Empty<int>();
			
			foreach (var operation in operations)
			{
				switch (operation?.Type)
				{
					case OperationType.Enqueue:
						var entryToAdd = new Entry(operation);
						lock (_entries) { _entries.AddLast(entryToAdd); }
						var itemCountAddition = _countOfItemsPerFile.GetValueOrDefault(entryToAdd.FileNumber);
						_countOfItemsPerFile[entryToAdd.FileNumber] = itemCountAddition + 1;
						break;

					case OperationType.Dequeue:
						var entryToRemove = new Entry(operation);
						lock (_checkedOutEntries) { _checkedOutEntries.Remove(entryToRemove); }
						var itemCountRemoval = _countOfItemsPerFile.GetValueOrDefault(entryToRemove.FileNumber);
						_countOfItemsPerFile[entryToRemove.FileNumber] = itemCountRemoval - 1;
						break;

					case OperationType.Reinstate:
						var entryToReinstate = new Entry(operation);
						lock (_entries ) { _entries.AddFirst(entryToReinstate); }
						lock (_checkedOutEntries) { _checkedOutEntries.Remove(entryToReinstate); }
						break;
				}
			}

			var filesToRemove = new HashSet<int>(
				from pair in _countOfItemsPerFile
				where pair.Value < 1
				select pair.Key
				);

			foreach (var i in filesToRemove)
			{
				_countOfItemsPerFile.Remove(i);
			}
			return filesToRemove.ToArray();
		}

        /// <summary>
        /// If 'throwOnConflict' was set in the constructor, throw an InvalidOperationException. This will stop program flow.
        /// If not, throw an EndOfStreamException, which should result in silent data truncation.
        /// </summary>
	    private void ThrowIfStrict(string msg)
	    {
	        if (_throwOnConflict)
	        {
	            throw new InvalidOperationException(msg);
	        }

	        throw new EndOfStreamException();   // silently truncate transactions
	    }

	    private void AssertTransactionSeparator(BinaryReader binaryReader, int txCount, Marker whichSeparator, Action hasData)
		{
			var bytes = binaryReader.ReadBytes(16);
			if (bytes.Length == 0) throw new EndOfStreamException();

			hasData();
			if (bytes.Length != 16)
			{
				// looks like we have a truncated transaction in this case, we will 
				// say that we run into end of stream and let the log trimming to deal with this
				if (binaryReader.BaseStream.Length == binaryReader.BaseStream.Position)
				{
					throw new EndOfStreamException();
				}
			    ThrowIfStrict("Unexpected data in transaction log. Expected to get transaction separator but got truncated data. Tx #" + txCount);
			}

		    Guid expectedValue, otherValue;
		    Marker otherSeparator;
		    if (whichSeparator == Marker.StartTransaction)
		    {
		        expectedValue = Constants.StartTransactionSeparatorGuid;
		        otherValue = Constants.EndTransactionSeparatorGuid;
		        otherSeparator = Marker.EndTransaction;
		    }
		    else if (whichSeparator == Marker.EndTransaction)
		    {
		        expectedValue = Constants.EndTransactionSeparatorGuid;
		        otherValue = Constants.StartTransactionSeparatorGuid;
		        otherSeparator = Marker.StartTransaction;
		    }
		    else throw new InvalidProgramException("Wrong kind of separator in inner implementation");

			var separator = new Guid(bytes);
		    if (separator != expectedValue)
		    {
		        if (separator == otherValue) // found a marker, but of the wrong type
		        {
		            ThrowIfStrict("Unexpected data in transaction log. Expected " + whichSeparator + " but found " + otherSeparator);
                }
		        ThrowIfStrict("Unexpected data in transaction log. Expected to get transaction separator but got unknown data. Tx #" + txCount);
		    }
		}


		private void ReadMetaState()
		{
			Atomic.Read(Meta, stream =>
			{
				using (var binaryReader = new BinaryReader(stream))
				{
					try
					{
						CurrentFileNumber = binaryReader.ReadInt32();
						CurrentFilePosition = binaryReader.ReadInt64();
					}
					catch (EndOfStreamException)
					{
					}
				}
			});
		}


		private void TrimTransactionLogIfNeeded(long txLogSize)
		{
			if (txLogSize < SuggestedMaxTransactionLogSize) return; // it is not big enough to care
			
			var optimalSize = GetOptimalTransactionLogSize();
			if (txLogSize < (optimalSize * 2)) return;  // not enough disparity to bother trimming
			
			FlushTrimmedTransactionLog();
		}

		private void ApplyTransactionOperations(IEnumerable<Operation> operations)
		{
			lock (_entries)
			{
				var filesToRemove = ApplyTransactionOperationsInMemory(operations);
				foreach (var fileNumber in filesToRemove)
				{
					if (CurrentFileNumber == fileNumber)
						continue;

					FileOperations.PrepareDelete(GetDataPath(fileNumber));
				}
			}
			FileOperations.Finalise();
		}

		private static byte[] GenerateTransactionBuffer(ICollection<Operation> operations)
		{
			byte[] transactionBuffer;
			using (var ms = new MemoryStream())
			{
				ms.Write(Constants.StartTransactionSeparator, 0, Constants.StartTransactionSeparator.Length);

				var count = BitConverter.GetBytes(operations.Count);
				ms.Write(count, 0, count.Length);

				foreach (var operation in operations)
				{
					WriteEntryToTransactionLog(ms, new Entry(operation), operation.Type);
				}
				ms.Write(Constants.EndTransactionSeparator, 0, Constants.EndTransactionSeparator.Length);

				ms.HardFlush();
				transactionBuffer = ms.ToArray();
			}
			return transactionBuffer;
		}

		private FileStream CreateWriter()
		{
			var dataFilePath = GetDataPath(CurrentFileNumber);
			var stream = new FileStream(
				dataFilePath,
				FileMode.OpenOrCreate,
				FileAccess.Write,
				FileShare.ReadWrite,
				0x10000,
				FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.WriteThrough);

			SetPermissions.TryAllowReadWriteForAll(dataFilePath);
			return stream;
		}

		public string GetDataPath(int index)
		{
			return Path.Combine(_path, "data." + index);
		}

		private long GetOptimalTransactionLogSize()
		{
			long size = 0;
			size += 16 /*sizeof(guid)*/; //	initial tx separator
			size += sizeof(int); // 	operation count

			size +=
				( // entry size == 16
				sizeof(int) + // 		operation separator
				sizeof(int) + // 		file number
				sizeof(int) + //		start
				sizeof(int) //		length
				)
				*
				(CurrentCountOfItemsInQueue);

			return size;
		}
	}
}