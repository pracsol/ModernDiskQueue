﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace ModernDiskQueue.Implementation
{
    /// <summary>
    /// A wrapper around System.IO.File to help with
    /// heavily multi-threaded and multi-process workflows
    /// </summary>
    internal class StandardFileDriver : IFileDriver
    {
        public const int RetryLimit = 10;

        private static readonly object _lock = new();
        private static readonly Queue<string> _waitingDeletes = new();

        public string GetFullPath(string path) => Path.GetFullPath(path);
        public string PathCombine(string a, string b) => Path.Combine(a, b);

        /// <summary>
        /// Test for the existence of a directory
        /// </summary>
        public bool DirectoryExists(string path)
        {
            lock (_lock)
            {
                return Directory.Exists(path);
            }
        }

        /// <summary>
        /// Moves a file to a temporary name and adds it to an internal
        /// delete list. Files are permanently deleted on a call to Finalise()
        /// </summary>
        public void PrepareDelete(string path)
        {
            lock (_lock)
            {
                if (!FileExists(path)) return;
                var dir = Path.GetDirectoryName(path) ?? "";
                var file = Path.GetFileNameWithoutExtension(path);
                var prefix = Path.GetRandomFileName();

                var deletePath = Path.Combine(dir, $"{file}_dc_{prefix}");

                if (Move(path, deletePath))
                {
                    _waitingDeletes.Enqueue(deletePath);
                }
            }
        }

        /// <summary>
        /// Commit any pending prepared operations.
        /// Each operation will happen in serial.
        /// </summary>
        public void Finalise()
        {
            lock (_lock)
            {
                while (_waitingDeletes.Count > 0)
                {
                    var path = _waitingDeletes.Dequeue();
                    if (path is null) continue;
                    File.Delete(path);
                }
            }
        }

        /// <summary>
        /// Create and open a new file with no sharing between processes.
        /// </summary>
#pragma warning disable IDE0079 // Suppress warning about suppressing warnings
#pragma warning disable CA1859 // Use concrete types when possible for improved performance
        private static ILockFile CreateNoShareFile(string path)
#pragma warning restore CA1859, IDE0079
        {
            lock (_lock)
            {
                var currentProcess = Process.GetCurrentProcess();
                var currentLockData = new LockFileData
                {
                    ProcessId = currentProcess.Id,
                    ThreadId = Environment.CurrentManagedThreadId,
                    ProcessStart = GetProcessStartAsUnixTimeMs(currentProcess),
                };
                var keyBytes = MarshallHelper.Serialize(currentLockData);

                if (!File.Exists(path)) File.WriteAllBytes(path, keyBytes);
                var lockBytes = File.ReadAllBytes(path); // will throw if OS considers the file locked
                var fileLockData = MarshallHelper.Deserialize<LockFileData>(lockBytes);

                if (fileLockData.ThreadId != currentLockData.ThreadId || fileLockData.ProcessId != currentLockData.ProcessId)
                {
                    // The first two *should not* happen, but filesystems seem to have weird bugs.
                    // Is this for our own process?
                    if (fileLockData.ProcessId == currentLockData.ProcessId)
                    {
                        throw new Exception($"This queue is locked by another thread in this process. Thread id = {fileLockData.ThreadId}");
                    }

                    // Is it for a running process?
                    if (IsRunning(fileLockData))
                    {
                        throw new Exception($"This queue is locked by another running process. Process id = {fileLockData.ProcessId}");
                    }

                    // We have a lock from a dead process. Kill it.
                    File.Delete(path);
                    File.WriteAllBytes(path, keyBytes);
                }

                var lockStream = new FileStream(path,
                    FileMode.Create,
                    FileAccess.ReadWrite,
                    FileShare.None);

                lockStream.Write(keyBytes, 0, keyBytes.Length);
                lockStream.Flush(true);

                return new LockFile(lockStream, path);
            }
        }

        /// <summary>
        /// Return true if the processId matches a running process
        /// </summary>
        private static bool IsRunning(LockFileData lockData)
        {
            try
            {
                var p = Process.GetProcessById(lockData.ProcessId);
                var startTimeOffset = GetProcessStartAsUnixTimeMs(p);
                return startTimeOffset == lockData.ProcessStart;
            }
            catch (InvalidOperationException)
            {
                return false;
            }
            catch (ArgumentException)
            {
                return false;
            }
            catch
            {
                return true;
            }
        }

        private static long GetProcessStartAsUnixTimeMs(Process process)
            => ((DateTimeOffset)process.StartTime).ToUnixTimeMilliseconds();

        /// <summary>
        /// Test for the existence of a file
        /// </summary>
        public bool FileExists(string path)
        {
            lock (_lock)
            {
                return File.Exists(path);
            }
        }

        public void DeleteRecursive(string path)
        {
            lock (_lock)
            {
                if (Path.GetPathRoot(path) == Path.GetFullPath(path)) throw new Exception("Request to delete root directory rejected");
                if (string.IsNullOrWhiteSpace(Path.GetDirectoryName(path)!)) throw new Exception("Request to delete root directory rejected");
                if (File.Exists(path)) throw new Exception("Tried to recursively delete a single file.");

                Directory.Delete(path, true);
            }
        }

        public Maybe<ILockFile> CreateLockFile(string path)
        {
            try
            {
                return CreateNoShareFile(path).Success();
            }
            catch (Exception ex)
            {
                return Maybe<ILockFile>.Fail(ex);
            }
        }

        public void ReleaseLock(ILockFile fileLock)
        {
            lock (_lock)
            {
                fileLock.Dispose();
            }
        }

        /// <summary>
        /// Attempt to create a directory. No error if the directory already exists.
        /// </summary>
        public void CreateDirectory(string path)
        {
            lock (_lock)
            {
                Directory.CreateDirectory(path);
            }
        }

        /// <summary>
        /// Rename a file, including its path
        /// </summary>
        private static bool Move(string oldPath, string newPath)
        {
            lock (_lock)
            {
                for (var i = 0; i < RetryLimit; i++)
                {
                    try
                    {
                        File.Move(oldPath, newPath);
                        return true;
                    }
                    catch
                    {
                        Thread.Sleep(i * 100);
                    }
                }
            }
            return false;
        }

        public IFileStream OpenTransactionLog(string path, int bufferLength)
        {
            lock (_lock)
            {
                var stream = new FileStream(path,
                    FileMode.Append,
                    FileAccess.Write,
                    FileShare.None,
                    bufferLength,
                    FileOptions.SequentialScan | FileOptions.WriteThrough);

                return new FileStreamWrapper(stream);
            }
        }

        public IFileStream OpenReadStream(string path)
        {
            lock (_lock)
            {
                var stream = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite);
                return new FileStreamWrapper(stream);
            }
        }

        public IFileStream OpenWriteStream(string dataFilePath)
        {
            lock (_lock)
            {
                var stream = new FileStream(
                    dataFilePath,
                    FileMode.OpenOrCreate,
                    FileAccess.Write,
                    FileShare.ReadWrite,
                    0x10000,
                    FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.WriteThrough);

                SetPermissions.TryAllowReadWriteForAll(dataFilePath);
                return new FileStreamWrapper(stream);
            }
        }

        public bool AtomicRead(string path, Action<IBinaryReader> action)
        {
            for (int i = 1; i <= RetryLimit; i++)
            {
                try
                {
                    AtomicReadInternal(path, fileStream =>
                    {
                        var wrapper = new FileStreamWrapper(fileStream);
                        action(wrapper);
                    });
                    return true;
                }
                catch (UnrecoverableException)
                {
                    throw;
                }
                catch (Exception)
                {
                    if (i >= RetryLimit)
                    {
                        PersistentQueue.Log("Exceeded retry limit during read");
                        return false;
                    }

                    Thread.Sleep(i * 100);
                }
            }
            return false;
        }

        public void AtomicWrite(string path, Action<IBinaryWriter> action)
        {
            for (int i = 1; i <= RetryLimit; i++)
            {
                try
                {
                    AtomicWriteInternal(path, fileStream =>
                    {
                        var wrapper = new FileStreamWrapper(fileStream);
                        action(wrapper);
                    });
                    return;
                }
                catch (Exception) when (i >= RetryLimit)
                {
                    throw;
                }
                catch (Exception)
                {
                    Thread.Sleep(i * 100);
                }
            }
        }

        /// <summary>
		/// Run a read action over a file by name.
		/// Access is optimised for sequential scanning.
		/// No file share is permitted.
		/// </summary>
		/// <param name="path">File path to read</param>
		/// <param name="action">Action to consume file stream. You do not need to close the stream yourself.</param>
        private void AtomicReadInternal(string path, Action<FileStream> action)
        {
            lock (_lock)
            {
                if (FileExists(path + ".old_copy")) WaitDelete(path);

                using var stream = new FileStream(path,
                    FileMode.OpenOrCreate,
                    FileAccess.Read,
                    FileShare.ReadWrite,
                    0x10000,
                    FileOptions.SequentialScan);

                SetPermissions.TryAllowReadWriteForAll(path);
                action(stream);
            }
        }

        /// <summary>
        /// Run a write action to a file.
        /// This will always rewrite the file (no appending).
        /// </summary>
        /// <param name="path">File path to write</param>
        /// <param name="action">Action to write into file stream. You do not need to close the stream yourself.</param>
        private void AtomicWriteInternal(string path, Action<FileStream> action)
        {
            lock (_lock)
            {
                // if the old copy file exists, this means that we have
                // a previous corrupt write, so we will not overwrite it, but 
                // rather overwrite the current file and keep it as our backup.
                if (FileExists(path) && !FileExists(path + ".old_copy"))
                    Move(path, path + ".old_copy");

                var dir = Path.GetDirectoryName(path);
                if (dir is not null && !DirectoryExists(dir)) CreateDirectory(dir);

                using var stream = new FileStream(path,
                    FileMode.Create,
                    FileAccess.Write,
                    FileShare.ReadWrite,
                    0x10000,
                    FileOptions.WriteThrough | FileOptions.SequentialScan);

                SetPermissions.TryAllowReadWriteForAll(path);
                action(stream);
                HardFlush(stream);

                WaitDelete(path + ".old_copy");
            }
        }

        /// <summary>
        /// Flush a stream, checking to see if its a file -- in which case it will ask for a flush-to-disk.
        /// </summary>
        private static void HardFlush(Stream? stream)
        {
            if (stream == null) return;
            if (stream is FileStream fs) fs.Flush(true);
            stream.Flush();
        }

        private void WaitDelete(string s)
        {
            for (var i = 0; i < RetryLimit; i++)
            {
                try
                {
                    lock (_lock)
                    {
                        PrepareDelete(s);
                        Finalise();
                    }

                    return;
                }
                catch
                {
                    Thread.Sleep(100);
                }
            }
        }
    }

    /// <summary>
    /// Strict mode exceptions that can't be retried
    /// </summary>
    public class UnrecoverableException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the UnrecoverableException class
        /// </summary>
        public UnrecoverableException()
            : base("An unrecoverable error occurred during file operation")
        {
        }

        /// <summary>
        /// Initializes a new instance of the UnrecoverableException class with a specified error message
        /// </summary>
        public UnrecoverableException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the UnrecoverableException class with a specified error message
        /// and a reference to the inner exception that is the cause of this exception
        /// </summary>
        public UnrecoverableException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}