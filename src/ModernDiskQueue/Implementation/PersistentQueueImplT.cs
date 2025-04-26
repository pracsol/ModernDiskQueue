namespace ModernDiskQueue.Implementation
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using ModernDiskQueue.PublicInterfaces;

    /// <inheritdoc cref="IPersistentQueueImpl{T}"/>
    internal class PersistentQueueImpl<T> : PersistentQueueImpl, IPersistentQueueImpl<T>
    {
        public PersistentQueueImpl(string path) : base(path) { }
        public PersistentQueueImpl(string path, int maxFileSize, bool throwOnConflict) : base(path, maxFileSize, throwOnConflict) { }
        internal PersistentQueueImpl(ILoggerFactory loggerFactory, string path, int maxFileSize, bool throwOnConflict, bool isAsyncMode)
            : base(loggerFactory, path, maxFileSize, throwOnConflict, isAsyncMode) { }

        public new IPersistentQueueSession<T> OpenSession()
        {
            return new PersistentQueueSession<T>(_loggerFactory, this, CreateWriter(), SuggestedWriteBuffer, FileTimeoutMilliseconds);
        }

        public new async Task<IPersistentQueueSession<T>> OpenSessionAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new PersistentQueueSession<T>(_loggerFactory, this, await CreateWriterAsync(cancellationToken).ConfigureAwait(false), SuggestedWriteBuffer, FileTimeoutMilliseconds);
        }
    }
}