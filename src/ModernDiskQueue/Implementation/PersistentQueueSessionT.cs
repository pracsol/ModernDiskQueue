namespace ModernDiskQueue.Implementation
{
    using Microsoft.Extensions.Logging;
    using ModernDiskQueue;
    using ModernDiskQueue.Implementation.Interfaces;
    using System.Threading;
    using System.Threading.Tasks;

    /// <inheritdoc cref="IPersistentQueueSession{T}"/>
    public class PersistentQueueSession<T> : PersistentQueueSession, IPersistentQueueSession<T>
    {
        /// <inheritdoc cref="IPersistentQueueSession{T}"/>
        public ISerializationStrategy<T> SerializationStrategy { get; set; }

        /// <inheritdoc cref="IPersistentQueueSession{T}"/>
        public PersistentQueueSession(
            ILoggerFactory loggerFactory,
            IPersistentQueueImpl queue,
            IFileStream currentStream,
            int writeBufferSize,
            int timeoutLimit,
            ISerializationStrategy<T>? serializationStrategy = null)
            : base(loggerFactory, queue, currentStream, writeBufferSize, timeoutLimit)
        {
            SerializationStrategy = serializationStrategy ?? new SerializationStrategyXml<T>();
        }

        /// <inheritdoc cref="IPersistentQueueSession{T}"/>
        public void Enqueue(T data)
        {
            byte[]? bytes = SerializationStrategy.Serialize(data);
            if (bytes != null)
            {
                Enqueue(bytes);
            }
        }

        /// <inheritdoc cref="IPersistentQueueSession{T}"/>
        public async ValueTask EnqueueAsync(T data, CancellationToken cancellationToken = default)
        {
            byte[]? bytes = await SerializationStrategy.SerializeAsync(data, cancellationToken).ConfigureAwait(false);
            if (bytes != null)
            {
                await base.EnqueueAsync(bytes, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <inheritdoc cref="IPersistentQueueSession{T}"/>
        public new T? Dequeue()
        {
            byte[]? bytes = base.Dequeue();
            T? obj = SerializationStrategy.Deserialize(bytes);
            return obj;
        }

        /// <inheritdoc cref="IPersistentQueueSession{T}"/>
        public new async ValueTask<T?> DequeueAsync(CancellationToken cancellationToken = default)
        {
            byte[]? bytes = await base.DequeueAsync(cancellationToken).ConfigureAwait(false);
            return await SerializationStrategy.DeserializeAsync(bytes, cancellationToken).ConfigureAwait(false);
        }
    }
}
