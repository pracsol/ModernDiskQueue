namespace ModernDiskQueue
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    /// <summary>
    /// Queue session (exclusive use of the queue to add or remove items)
    /// The queue session should be wrapped in a `using`, as it must be disposed.
    /// If you are sharing access, you should hold the queue session for as little time as possible.
    /// </summary>
    public interface IPersistentQueueSession : IDisposable, IAsyncDisposable
    {
        /// <summary>
        /// Queue data for a later decode. Data is written on `Flush()`
        /// </summary>
        void Enqueue(byte[] data);

        /// <summary>
        /// Queue data for a later decode. Data is written on `Flush()`
        /// </summary>
        ValueTask EnqueueAsync(byte[] data, CancellationToken cancellationToken = default);

        /// <summary>
        /// Try to pull data from the queue. Data is removed from the queue on `Flush()`
        /// </summary>
        byte[]? Dequeue();

        /// <summary>
        /// Try to pull data from the queue. Data is removed from the queue on `Flush()`
        /// </summary>
        ValueTask<byte[]?> DequeueAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Commit actions taken in this session since last flush.
        /// If the session is disposed with no flush, actions are not persisted
        /// to the queue (Enqueues are not written, dequeues are left on the queue)
        /// </summary>
        void Flush();

        /// <summary>
        /// Commit actions taken in this session since last flush.
        /// If the session is disposed with no flush, actions are not persisted
        /// to the queue (Enqueues are not written, dequeues are left on the queue)
        /// </summary>
        ValueTask FlushAsync(CancellationToken cancellationToken = default);
    }
}
