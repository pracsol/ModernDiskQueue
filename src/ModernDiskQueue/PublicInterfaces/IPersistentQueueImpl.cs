using ModernDiskQueue.Implementation;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ModernDiskQueue
{
    /// <summary>
    /// Wrapper for exposing some inner workings of the persistent queue.
    /// <para>You should be careful using any of these methods</para>
    /// <para>Please read the source code before using these methods in production software</para>
    /// </summary>
    public interface IPersistentQueueImpl : IDisposable
    {
        /// <summary>
        /// <para>UNSAFE. Incorrect use will result in data loss.</para>
        /// Lock and process a data file writer at the current write head.
        /// <para>This will create new files if max size is exceeded</para>
        /// </summary>
        /// <param name="stream">Stream to write</param>
        /// <param name="action">Writing action</param>
        /// <param name="onReplaceStream">Continuation action if a new file is created</param>
        void AcquireWriter(IFileStream stream, Func<IFileStream, Task<long>> action, Action<IFileStream> onReplaceStream);

        /// <summary>
        /// <para>UNSAFE. Incorrect use will result in data loss.</para>
        /// Commit a sequence of operations to storage
        /// </summary>
        void CommitTransaction(ICollection<Operation> operations);

        /// <summary>
        /// <para>UNSAFE. Incorrect use will result in data loss.</para>
        /// Dequeue data, returning storage entry
        /// </summary>
        Entry? Dequeue();

        /// <summary>
        /// <para>UNSAFE. Incorrect use will result in data loss.</para>
        /// <para>Undo Enqueue and Dequeue operations.</para>
        /// <para>These MUST have been real operations taken.</para>
        /// </summary>
        void Reinstate(IEnumerable<Operation> reinstatedOperations);

        /// <summary>
        /// <para>Safe, available for tests and performance.</para>
        /// <para>Current writing file number</para>
        /// </summary>
        int CurrentFileNumber { get; }

        /// <summary>
        /// <para>Safe, available for tests and performance.</para>
        /// <para>If true, trim and flush waiting transactions on dispose</para>
        /// </summary>
        bool TrimTransactionLogOnDispose { get; set; }

        /// <summary>
        /// <para>Setting this to false may cause unexpected data loss in some failure conditions.</para>
        /// <para>Defaults to true.</para>
        /// <para>If true, each transaction commit will flush the transaction log.</para>
        /// <para>This is slow, but ensures the log is correct per transaction in the event of a hard termination (i.e. power failure)</para>
        /// </summary>
        bool ParanoidFlushing { get; set; }

        /// <summary>
        /// Setting this to true will prevent some file-system level errors from stopping the queue.
        /// <para>Only use this if uptime is more important than correctness of data</para>
        /// </summary>
        bool AllowTruncatedEntries { get; set; }

        /// <summary>
        /// Approximate size of the queue. Used for testing
        /// </summary>
        int EstimatedCountOfItemsInQueue { get; }

        /// <summary>
        /// Configured maximum size of each queue file.
        /// Files will be split and rolled after this limit.
        /// </summary>
        int MaxFileSize { get; }

        /// <summary>
        /// If the transaction log is near this size, it will be flushed and trimmed.
        /// If you set Internals.ParanoidFlushing, this value is ignored.
        /// </summary>
        long SuggestedMaxTransactionLogSize { get; set; }

        /// <summary>
        /// Override the file access mechanism. For test and advanced users only.
        /// See the source code for more details.
        /// </summary>
        void SetFileDriver(IFileDriver writeFailureDriver);

        /// <summary>
        /// Lock the queue for use, and give access to session methods.
        /// The session <b>MUST</b> be disposed as soon as possible.
        /// </summary>
        IPersistentQueueSession OpenSession();

        /// <summary>
        /// WARNING: 
        /// Attempt to delete the queue, all its data, and all support files.
        /// </summary>
        void HardDelete(bool reset);
    }
}