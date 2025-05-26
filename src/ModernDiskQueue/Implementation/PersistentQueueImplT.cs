namespace ModernDiskQueue.Implementation
{
    using Microsoft.Extensions.Logging;
    using ModernDiskQueue;
    using ModernDiskQueue.Implementation.Interfaces;
    using System.Threading;
    using System.Threading.Tasks;

    /// <inheritdoc cref="IPersistentQueueImpl{T}"/>
    internal class PersistentQueueImpl<T> : PersistentQueueImpl, IPersistentQueueImpl<T>
    {
        public PersistentQueueImpl(string path) : base(path) { }
        public PersistentQueueImpl(string path, int maxFileSize, bool throwOnConflict) : base(path, maxFileSize, throwOnConflict) { }
        internal PersistentQueueImpl(ILoggerFactory loggerFactory, string path, int maxFileSize, bool throwOnConflict, bool isAsyncMode, ModernDiskQueueOptions options, IFileDriver fileDriver)
            : base(loggerFactory, path, maxFileSize, throwOnConflict, isAsyncMode, options, fileDriver) { }

        public new IPersistentQueueSession<T> OpenSession()
        {
            return new PersistentQueueSession<T>(_loggerFactory, this, CreateWriter(), SuggestedWriteBuffer, FileTimeoutMilliseconds, DefaultSerializationStrategy);
        }

        public new async Task<IPersistentQueueSession<T>> OpenSessionAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new PersistentQueueSession<T>(_loggerFactory, this, await CreateWriterAsync(cancellationToken).ConfigureAwait(false), SuggestedWriteBuffer, FileTimeoutMilliseconds, DefaultSerializationStrategy);
        }

        public async Task<IPersistentQueueSession<T>> OpenSessionAsync(ISerializationStrategy<T>? serializationStrategy, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new PersistentQueueSession<T>(_loggerFactory, this, await CreateWriterAsync(cancellationToken).ConfigureAwait(false), SuggestedWriteBuffer, FileTimeoutMilliseconds, serializationStrategy);
        }

        /// <inheritdoc/>
        public async Task<IPersistentQueueSession<T>> OpenSessionAsync(SerializationStrategy serializationStrategy, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new PersistentQueueSession<T>(_loggerFactory, this, await CreateWriterAsync(cancellationToken).ConfigureAwait(false), SuggestedWriteBuffer, FileTimeoutMilliseconds, (serializationStrategy == SerializationStrategy.Xml) ? new SerializationStrategyXml<T>() : new SerializationStrategyJson<T>());
        }

        /// <summary>
        /// The default serialization strategy used by the opensessionasync methods will be set to this.
        /// </summary>
        public ISerializationStrategy<T> DefaultSerializationStrategy { get; set; } = new SerializationStrategyXml<T>();
    }
}