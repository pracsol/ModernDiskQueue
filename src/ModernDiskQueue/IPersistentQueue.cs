namespace ModernDiskQueue
{
    using ModernDiskQueue.Implementation.Interfaces;
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// A queue tied to a specific persistent storage backing.
    /// Enqueue and dequeue operations happen within sessions.
    /// <example>using (var session = q.OpenSession()) {...}</example>
    /// Queue should be disposed after use. This will NOT destroy the backing storage.
    /// </summary>
    public interface IPersistentQueue : IDisposable, IAsyncDisposable
    {
        /// <summary>
        /// Open an read/write session
        /// </summary>
        IPersistentQueueSession OpenSession();

        /// <summary>
        /// Asynchronously opens a read/write session with the queue
        /// </summary>
        /// <param name="cancellationToken">Token to monitor for cancellation requests</param>
        /// <returns>A persistent queue session that can be used to read from and write to the queue</returns>
        Task<IPersistentQueueSession> OpenSessionAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Returns the number of items in the queue, but does not include items added or removed
        /// in currently open sessions.
        /// </summary>
        int EstimatedCountOfItemsInQueue { get; }

        /// <summary>
        /// Returns the number of items in the queue, but does not include items added or removed
        /// in currently open sessions.
        /// </summary>
        Task<int> GetEstimatedCountOfItemsInQueueAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Advanced adjustable settings. Use with caution. Read the source code.
        /// </summary>
        IPersistentQueueImpl Internals { get; }

        /// <summary>
        /// Maximum size of files in queue. New files will be rolled-out if this is exceeded.
        /// (i.e. this is NOT the maximum size of the queue)
        /// </summary>
        int MaxFileSize { get; }

        /// <summary>
        /// If the transaction log is near this size, it will be flushed and trimmed.
        /// If you set Internals.ParanoidFlushing, this value is ignored.
        /// </summary>
        long SuggestedMaxTransactionLogSize { get; set; }

        /// <summary>
        /// Defaults to true.
        /// If true, transactions will be flushed and trimmed on Dispose (makes dispose a bit slower)
        /// If false, transaction log will be left as-is on Dispose.
        /// </summary>
        bool TrimTransactionLogOnDispose { get; set; }

        /// <summary>
        /// WARNING: Dangerous!
        /// Attempt to delete the queue, all its data, and all support files.
        /// This is not thread safe, multi-process safe, or safe in any other way.
        ///<para></para>
        /// If reset is true, the queue's directory and lock file will be restored, and the queue can continue to be used.
        /// </summary>
        void HardDelete(bool reset);

        /// <summary>
        /// WARNING: Dangerous!
        /// Asynchronously attempts to delete the queue, all its data, and all support files.
        /// This is not thread safe, multi-process safe, or safe in any other way.
        /// <para></para>
        /// If reset is true, the queue's directory and lock file will be restored, and the queue can continue to be used.
        /// </summary>
        Task HardDeleteAsync(bool reset, CancellationToken cancellationToken = default);
    }
}