namespace ModernDiskQueue
{
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.Abstractions;
    using ModernDiskQueue.Implementation;
    using ModernDiskQueue.Implementation.Interfaces;
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <inheritdoc />
    public class PersistentQueue<T> : PersistentQueue, IPersistentQueue<T>
    {
        private readonly ILoggerFactory _loggerFactory = NullLoggerFactory.Instance;
        private readonly ILogger<PersistentQueue<T>> _logger = NullLogger<PersistentQueue<T>>.Instance;

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

        internal PersistentQueue(ILoggerFactory loggerFactory, IPersistentQueueImpl<T> queue)
        {
            _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
            _logger = loggerFactory?.CreateLogger<PersistentQueue<T>>() ?? NullLogger<PersistentQueue<T>>.Instance;
            Queue = queue;
        }

        /// <summary>
        /// Sets the ILogger instance.
        /// </summary>
        protected override ILogger Logger => _logger;

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

        /// <summary>
        /// Open a read/write session asynchronously.
        /// </summary>
        /// <remarks>
        /// This class implements <see cref="IAsyncDisposable"/>. Always use <c>await using</c>
        /// instead of <c>using</c> with async methods to ensure proper asynchronous resource cleanup.
        /// </remarks>
        /// <param name="serializationStrategy">Specify a custom serialization strategy.</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/>.</param>
        /// <returns><see cref="IPersistentQueueSession{T}"/></returns>
        public async Task<IPersistentQueueSession<T>> OpenSessionAsync(ISerializationStrategy<T> serializationStrategy, CancellationToken cancellationToken = default)
        {
            if (Queue == null) throw new Exception("This queue has been disposed");
            if (Queue is PersistentQueueImpl<T> typedQueue)
            {
                // Get the base session from the async call
                var baseSession = await typedQueue.OpenSessionAsync(serializationStrategy, cancellationToken).ConfigureAwait(false);

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