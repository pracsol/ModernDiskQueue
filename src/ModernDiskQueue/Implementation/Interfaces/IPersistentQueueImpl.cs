using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ModernDiskQueue.Implementation.Interfaces
{
    /// <summary>
    /// Wrapper for exposing some inner workings of the persistent queue.
    /// <para>You should be careful using any of these methods</para>
    /// <para>Please read the source code before using these methods in production software</para>
    /// </summary>
    public interface IPersistentQueueImpl : IDisposable, IAsyncDisposable
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
        /// Asynchronously lock and process a data file writer at the current write head.
        /// <para>This will create new files if max size is exceeded</para>
        /// </summary>
        /// <param name="stream">Stream to write</param>
        /// <param name="action">Asynchronous writing action</param>
        /// <param name="onReplaceStream">Continuation action if a new file is created</param>
        /// <param name="cancellationToken">Token to monitor for cancellation requests</param>
        /// <returns>A task that represents the asynchronous operation</returns>
        Task AcquireWriterAsync(IFileStream stream, Func<IFileStream, CancellationToken, Task<long>> action, Action<IFileStream> onReplaceStream, CancellationToken cancellationToken = default);

        /// <summary>
        /// <para>UNSAFE. Incorrect use will result in data loss.</para>
        /// Commit a sequence of operations to storage
        /// </summary>
        void CommitTransaction(ICollection<Operation> operations);

        /// <summary>
        /// <para>UNSAFE. Incorrect use will result in data loss.</para>
        /// Asynchronously commit a sequence of operations to storage
        /// </summary>
        /// <param name="operations">Collection of operations to commit</param>
        /// <param name="cancellationToken">Token to monitor for cancellation requests</param>
        /// <returns>A task that represents the asynchronous operation</returns>
        ValueTask CommitTransactionAsync(ICollection<Operation> operations, CancellationToken cancellationToken = default);

        /// <summary>
        /// <para>UNSAFE. Incorrect use will result in data loss.</para>
        /// Dequeue data, returning storage entry
        /// </summary>
        Entry? Dequeue();

        /// <summary>
        /// <para>UNSAFE. Incorrect use will result in data loss.</para>
        /// Asynchronously dequeue data, returning storage entry
        /// </summary>
        /// <param name="cancellationToken">Token to monitor for cancellation requests</param>
        /// <returns>A task that represents the asynchronous operation with the dequeued entry or null</returns>
        ValueTask<Entry?> DequeueAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// <para>UNSAFE. Incorrect use will result in data loss.</para>
        /// <para>Undo Enqueue and Dequeue operations.</para>
        /// <para>These MUST have been real operations taken.</para>
        /// </summary>
        void Reinstate(IEnumerable<Operation> reinstatedOperations);

        /// <summary>
        /// <para>UNSAFE. Incorrect use will result in data loss.</para>
        /// <para>Asynchronously undo Enqueue and Dequeue operations.</para>
        /// <para>These MUST have been real operations taken.</para>
        /// </summary>
        /// <param name="reinstatedOperations">Operations to reinstate</param>
        /// <param name="cancellationToken">Token to monitor for cancellation requests</param>
        /// <returns>A task that represents the asynchronous operation</returns>
        ValueTask ReinstateAsync(IEnumerable<Operation> reinstatedOperations, CancellationToken cancellationToken = default);

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
        /// Gets or sets the timeout value for file operations.
        /// </summary>
        /// <remarks>
        /// Maximum time for IO operations (including read &amp; write) to complete.
        /// If any individual operation takes longer than this, an exception will occur.
        /// </remarks>
        int FileTimeoutMilliseconds { get; set; }

        /// <summary>
        /// Gets or sets the file stream sharing option.
        /// </summary>
        /// <remarks>
        /// <p>This setting allows sharing of the queue file across multiple processes and users. You
        /// will probably want to set this to <c>true</c> if you are synchronising across containers
        /// or are using network storage.</p>
        /// <p>If true, files that are created will be given read/write access for all users</p>
        /// <p>If false, files that are created will be left at default permissions of the running process</p>
        /// </remarks>
        bool SetFilePermissions { get; set; }

        /// <summary>
        /// Approximate size of the queue. Used for testing
        /// </summary>
        int EstimatedCountOfItemsInQueue { get; }

        /// <summary>
		/// Approximate size of the queue. Used for testing
		/// </summary>
        ValueTask<int> GetEstimatedCountOfItemsInQueueAsync(CancellationToken cancellationToken = default);

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
        /// Asynchronously lock the queue for use, and give access to session methods.
        /// The session <b>MUST</b> be disposed as soon as possible.
        /// </summary>
        /// <param name="cancellationToken">Token to monitor for cancellation requests</param>
        /// <returns>A task that represents the asynchronous operation with a session that can be used to interact with the queue</returns>
        ValueTask<IPersistentQueueSession> OpenSessionAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// WARNING:
        /// Attempt to delete the queue, all its data, and all support files.
        /// </summary>
        void HardDelete(bool reset);

        /// <summary>
        /// WARNING:
        /// Asynchronously attempt to delete the queue, all its data, and all support files.
        /// </summary>
        /// <param name="reset">If true, recreate queue storage</param>
        /// <param name="cancellationToken">Token to monitor for cancellation requests</param>
        /// <returns>A task that represents the asynchronous operation</returns>
        Task HardDeleteAsync(bool reset, CancellationToken cancellationToken = default);
    }
}