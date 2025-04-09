using ModernDiskQueue.Implementation;
using ModernDiskQueue.PublicInterfaces;
using System;
using System.Threading.Tasks;
using System.Threading;

namespace ModernDiskQueue
{
    /// <inheritdoc cref="IPersistentQueue{T}" />
    public class PersistentQueue<T> : PersistentQueue, IPersistentQueue<T>
    {
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