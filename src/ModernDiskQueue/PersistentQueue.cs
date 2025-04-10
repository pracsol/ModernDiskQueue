﻿using ModernDiskQueue.Implementation;
using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;

namespace ModernDiskQueue
{
    /// <summary>
    /// Default persistent queue <see cref="IPersistentQueue"/>
    /// <para>This queue establishes exclusive use of the storage until it is disposed.</para>
    /// <para>If you wish to share the store between processes, you should use <see cref="WaitFor(string,System.TimeSpan)"/> or <see cref="WaitFor{T}(string,System.TimeSpan)"/>.</para>
    /// <para>If you want to share the store between threads in one process, you may share the Persistent Queue and
    /// have each thread call `OpenSession` for itself.</para>
    /// </summary>
    [SuppressMessage("ReSharper", "UnusedMember.Global")]
    public class PersistentQueue : IPersistentQueue
    {
        /// <summary>
        /// The queue implementation instance, or null if not connected
        /// </summary>
        protected IPersistentQueueImpl? Queue;

        /// <summary>
        /// Logging action for non-critical faults. Defaults to Console.WriteLine.
        /// </summary>
        public static Action<string> Log { get; set; } = Console.WriteLine;

        private static T WaitFor<T>(Func<T> generator, TimeSpan maxWait, string lockName)
        {
            var sw = new Stopwatch();
            try
            {
                sw.Start();
                do
                {
                    try
                    {
                        return generator();
                    }
                    catch (DirectoryNotFoundException)
                    {
                        throw new Exception("Target storagePath does not exist or is not accessible");
                    }
                    catch (PlatformNotSupportedException ex)
                    {
                        Log("Blocked by " + ex.GetType()?.Name + "; " + ex.Message + Environment.NewLine + Environment.NewLine + ex.StackTrace);
                        throw;
                    }
                    catch
                    {
                        Thread.Sleep(50);
                    }
                } while (sw.Elapsed < maxWait);
            }
            finally
            {
                sw.Stop();
            }
            throw new TimeoutException($"Could not acquire a lock on '{lockName}' in the time specified");
        }

        /// <summary>
        /// Wait a maximum time to open an exclusive session.
        /// The queue is opened with default max file size (32MiB) and conflicts set to throw exceptions.
        /// <para>If sharing storage between processes, the resulting queue should disposed
        /// as soon as possible.</para>
        /// <para>Throws a TimeoutException if the queue can't be locked in the specified time</para>
        /// </summary>
        /// <exception cref="TimeoutException">Lock on file could not be acquired</exception>
        /// <param name="storagePath">Directory path for queue storage. This will be created if it doesn't already exist</param>
        /// <param name="maxWait">If the storage path can't be locked within this time, a TimeoutException will be thrown</param>
        public static IPersistentQueue WaitFor(string storagePath, TimeSpan maxWait)
        {
            return WaitFor(() => new PersistentQueue(storagePath), maxWait, storagePath);
        }

        /// <summary>
        /// Wait a maximum time to open an exclusive session.
        /// <para>If sharing storage between processes, the resulting queue should disposed
        /// as soon as possible.</para>
        /// <para>Throws a TimeoutException if the queue can't be locked in the specified time</para>
        /// </summary>
        /// <exception cref="TimeoutException">Lock on file could not be acquired</exception>
        /// <param name="storagePath">Directory path for queue storage. This will be created if it doesn't already exist</param>
        /// <param name="throwOnConflict">When true, if data files are damaged, throw an InvalidOperationException. This will stop program flow.
        /// When false, damaged data files should result in silent data truncation</param>
        /// <param name="maxWait">If the storage path can't be locked within this time, a TimeoutException will be thrown</param>
        /// <param name="maxSize">Maximum size in bytes for each storage file. Files will be rotated after reaching this limit.
        /// The entire queue is NOT limited by this value.</param>
        public static IPersistentQueue WaitFor(string storagePath, int maxSize, bool throwOnConflict, TimeSpan maxWait)
        {
            return WaitFor(() => new PersistentQueue(storagePath, maxSize, throwOnConflict), maxWait, storagePath);
        }

        /// <summary>
        /// Wait a maximum time to open an exclusive session.
        /// The queue is opened with default max file size (32MiB) and conflicts set to throw exceptions.
        /// <para>If sharing storage between processes, the resulting queue should disposed
        /// as soon as possible.</para>
        /// <para>Throws a TimeoutException if the queue can't be locked in the specified time</para>
        /// </summary>
        /// <exception cref="TimeoutException">Lock on file could not be acquired</exception>
        public static IPersistentQueue<T> WaitFor<T>(string storagePath, TimeSpan maxWait)
        {
            return WaitFor(() => new PersistentQueue<T>(storagePath), maxWait, storagePath);
        }

        /// <summary>
        /// Wait a maximum time to open an exclusive session.
        /// <para>If sharing storage between processes, the resulting queue should disposed
        /// as soon as possible.</para>
        /// <para>Throws a TimeoutException if the queue can't be locked in the specified time</para>
        /// </summary>
        /// <exception cref="TimeoutException">Lock on file could not be acquired</exception>
        /// <param name="storagePath">Directory path for queue storage. This will be created if it doesn't already exist</param>
        /// <param name="throwOnConflict">When true, if data files are damaged, throw an InvalidOperationException. This will stop program flow.
        /// When false, damaged data files should result in silent data truncation</param>
        /// <param name="maxWait">If the storage path can't be locked within this time, a TimeoutException will be thrown</param>
        /// <param name="maxSize">Maximum size in bytes for each storage file. Files will be rotated after reaching this limit.
        /// The entire queue is NOT limited by this value.</param>
        public static IPersistentQueue<T> WaitFor<T>(string storagePath, int maxSize, bool throwOnConflict, TimeSpan maxWait)
        {
            return WaitFor(() => new PersistentQueue<T>(storagePath, maxSize, throwOnConflict), maxWait, storagePath);
        }

        /// <summary>
        /// This constructor is only for use by derived classes.
        /// </summary>
        protected PersistentQueue() { }

        /// <summary>
        /// Create or connect to a persistent store at the given storage path.
        /// <para>Throws UnauthorizedAccessException if you do not have read and write permissions.</para>
        /// <para>Throws InvalidOperationException if another instance is attached to the backing store.</para>
        /// </summary>
        public PersistentQueue(string storagePath)
        {
            Queue = new PersistentQueueImpl(storagePath);
        }

        /// <summary>
        /// Create or connect to a persistent store at the given storage path.
        /// Uses specific maximum file size (files will be split if they exceed this size).
        /// <para>Throws UnauthorizedAccessException if you do not have read and write permissions.</para>
        /// <para>Throws InvalidOperationException if another instance is attached to the backing store.</para>
        /// If `throwOnConflict` is set to false, data corruption will be silently ignored. Use this only where uptime is more important than data integrity.
        /// </summary>
        public PersistentQueue(string storagePath, int maxSize, bool throwOnConflict = true)
        {
            Queue = new PersistentQueueImpl(storagePath, maxSize, throwOnConflict);
        }

        /// <summary>
        /// Close this queue connection. Does not destroy flushed data.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2213:DisposableFieldsShouldBeDisposed", MessageId = "_queue", Justification = "Disposed in an interlock")]
        public void Dispose()
        {
            var local = Interlocked.Exchange(ref Queue, null);
            if (local == null) return;
            local.Dispose();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Dispose of the queue connection on destruction.
        /// This is a safety valve. You should ensure you dispose
        /// of connections properly.
        /// </summary>
        ~PersistentQueue()
        {
            if (Queue == null) return;
            Dispose();
        }

        /// <summary>
        /// Open an read/write session
        /// </summary>
        public IPersistentQueueSession OpenSession()
        {
            if (Queue == null) throw new Exception("This queue has been disposed");
            return Queue.OpenSession();
        }

        /// <summary>
        /// Returns the number of items in the queue, but does not include items added or removed
        /// in currently open sessions.
        /// </summary>
        public int EstimatedCountOfItemsInQueue => Queue?.EstimatedCountOfItemsInQueue ?? 0;

        /// <summary>
        /// Advanced adjustable settings. Use with caution. Read the source code.
        /// </summary>
        public IPersistentQueueImpl Internals => Queue ?? throw new InvalidOperationException("Internals not available in this state");

        /// <summary>
        /// Maximum size of files in queue. New files will be rolled-out if this is exceeded.
        /// (i.e. this is NOT the maximum size of the queue)
        /// </summary>
        public int MaxFileSize => Queue?.MaxFileSize ?? 0;

        /// <summary>
        /// WARNING: 
        /// Attempt to delete the queue, all its data, and all support files.
        /// </summary>
        public void HardDelete(bool reset)
        {
            if (Queue is null) throw new Exception("This queue has been disposed");
            Queue.HardDelete(reset);
        }

        /// <summary>
        /// If the transaction log is near this size, it will be flushed and trimmed.
        /// If you set Internals.ParanoidFlushing, this value is ignored.
        /// </summary>
        public long SuggestedMaxTransactionLogSize
        {
            get => Queue?.SuggestedMaxTransactionLogSize ?? 0;
            set { if (Queue != null) Queue.SuggestedMaxTransactionLogSize = value; }
        }

        /// <summary>
        /// Defaults to true.
        /// If true, transactions will be flushed and trimmed on Dispose (makes dispose a bit slower)
        /// If false, transaction log will be left as-is on Dispose.
        /// </summary>
        public bool TrimTransactionLogOnDispose
        {
            get => Queue?.TrimTransactionLogOnDispose ?? true;
            set { if (Queue != null) Queue.TrimTransactionLogOnDispose = value; }
        }

        /// <summary>
        /// Static settings that affect all queue instances created in this process
        /// </summary>
        public static class DefaultSettings
        {
            /// <summary>
            /// Initial setting: false
            /// <p>This setting allows sharing of the queue file across multiple processes and users. You
            /// will probably want to set this to <c>true</c> if you are synchronising across containers
            /// or are using network storage.</p>
            /// <p>If true, files that are created will be given read/write access for all users</p>
            /// <p>If false, files that are created will be left at default permissions of the running process</p>
            /// </summary>
            public static bool SetFilePermissions { get; set; } = false;

            /// <summary>
            /// Initial setting: false
            /// <para>Setting this to true will prevent some file-system level errors from stopping the queue.</para>
            /// <para>Only use this if uptime is more important than correctness of data</para>
            /// </summary>
            public static bool AllowTruncatedEntries { get; set; }

            /// <summary>
            /// Initial setting: true
            /// <para>Safe, available for tests and performance.</para>
            /// <para>If true, trim and flush waiting transactions on dispose</para>
            /// </summary>
            public static bool TrimTransactionLogOnDispose { get; set; } = true;

            /// <summary>
            /// Initial setting: true
            /// <para>Setting this to false may cause unexpected data loss in some failure conditions.</para>
            /// <para>If true, each transaction commit will flush the transaction log.</para>
            /// <para>This is slow, but ensures the log is correct per transaction in the event of a hard termination (i.e. power failure)</para>
            /// </summary>
            public static bool ParanoidFlushing { get; set; } = true;

            /// <summary>
            /// Initial setting: 10000 (10 sec).
            /// Maximum time for IO operations (including read &amp; write) to complete.
            /// If any individual operation takes longer than this, an exception will occur.
            /// </summary>
            public static int FileTimeoutMilliseconds { get; set; } = 10_000;
        }
    }
}