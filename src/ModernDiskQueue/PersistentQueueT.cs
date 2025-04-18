using ModernDiskQueue.Implementation;
using ModernDiskQueue.PublicInterfaces;
using System;
using System.Threading.Tasks;
using System.Threading;

namespace ModernDiskQueue
{
    /// <inheritdoc />
    public class PersistentQueue<T> : PersistentQueue, IPersistentQueue<T>
    {
        private PersistentQueue() { }

        /// <inheritdoc />
        public PersistentQueue(string storagePath)
        {
            Queue = new PersistentQueueImpl<T>(storagePath);
        }

        /// <inheritdoc />
        public PersistentQueue(string storagePath, int maxSize, bool throwOnConflict = true)
        {
            Queue = new PersistentQueueImpl<T>(storagePath, maxSize, throwOnConflict);
        }

        /// <summary>
        /// Create a new queue instance.
        /// </summary>
        /// <remarks>
        /// This class implements <see cref="IAsyncDisposable"/>. Always use <c>await using</c>
        /// instead of <c>using</c> with async methods to ensure proper asynchronous resource cleanup.
        /// </remarks>
        /// <param name="storagePath">Path to the directory facilitating the storage queue.</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/>.</param>
        /// <returns><see cref="PersistentQueue{T}"/></returns>
        public new static async Task<PersistentQueue<T>> CreateAsync(string storagePath, CancellationToken cancellationToken = default)
        {
            // Create a new instance but don't initialize the queue yet
            PersistentQueue<T> instance = new()
            {
                // Use the async factory method to initialize the queue
                Queue = await PersistentQueueImpl<T>.CreateAsync(storagePath, cancellationToken).ConfigureAwait(false),
            };
            return instance;
        }

        /// <summary>
        /// Create a new queue instance.
        /// </summary>
        /// <remarks>
        /// This class implements <see cref="IAsyncDisposable"/>. Always use <c>await using</c>
        /// instead of <c>using</c> with async methods to ensure proper asynchronous resource cleanup.
        /// </remarks>
        /// <param name="storagePath">Path to the directory facilitating the storage queue.</param>
        /// <param name="maxSize">Maximum size of the queue file.</param>
        /// <param name="throwOnConflict"><see cref="int"/></param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/>.</param>
        /// <returns><see cref="PersistentQueue{T}"/></returns>
        public new static async Task<PersistentQueue<T>> CreateAsync(string storagePath, int maxSize, bool throwOnConflict = true, CancellationToken cancellationToken = default)
        {
            PersistentQueue<T> instance = new()
            {
                Queue = await PersistentQueueImpl<T>.CreateAsync(storagePath, maxSize, throwOnConflict, cancellationToken).ConfigureAwait(false),
            };
            // Use the async factory method to initialize the queue
            return instance;
        }

        /// <summary>
        /// Open a read/write session
        /// </summary>
        public new IPersistentQueueSession<T> OpenSession()
        {
            if (Queue == null) throw new Exception("This queue has been disposed");
            return ((PersistentQueueImpl<T>)Queue).OpenSession();
        }

        /// <summary>
        /// Open a read/write session asynchronously.
        /// </summary>
        /// <remarks>
        /// This class implements <see cref="IAsyncDisposable"/>. Always use <c>await using</c>
        /// instead of <c>using</c> with async methods to ensure proper asynchronous resource cleanup.
        /// </remarks>
        /// <param name="cancellationToken"><see cref="CancellationToken"/>.</param>
        /// <returns><see cref="IPersistentQueueSession{T}"/></returns>
        public new async Task<IPersistentQueueSession<T>> OpenSessionAsync(CancellationToken cancellationToken = default)
        {
            if (Queue == null) throw new Exception("This queue has been disposed");
            if (Queue is PersistentQueueImpl<T> typedQueue)
            {
                // Get the base session from the async call
                var baseSession = await typedQueue.OpenSessionAsync(cancellationToken).ConfigureAwait(false);

                // Cast it to the generic version
                if (baseSession is IPersistentQueueSession<T> genericSession)
                {
                    return genericSession;
                }
                else
                {
                    throw new InvalidOperationException("Session created is not compatible with IPersistentQueueSession<T>");
                }
            }
            else
            {
                throw new InvalidOperationException("Queue is not of type PersistentQueueImpl<T>");
            }
        }
    }
}