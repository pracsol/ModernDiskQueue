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

        internal PersistentQueue(IPersistentQueueImpl<T> queue)
        {
            Queue = queue;
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