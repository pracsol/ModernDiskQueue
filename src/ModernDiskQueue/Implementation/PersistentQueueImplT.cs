namespace ModernDiskQueue.Implementation
{
    using System.Threading;
    using System.Threading.Tasks;
    using ModernDiskQueue.PublicInterfaces;

    /// <inheritdoc cref="IPersistentQueueImpl{T}"/>
    internal class PersistentQueueImpl<T> : PersistentQueueImpl, IPersistentQueueImpl<T>
    {
        private volatile bool _disposed;

        public PersistentQueueImpl(string path) : base(path) { }
        public PersistentQueueImpl(string path, int maxFileSize, bool throwOnConflict) : base(path, maxFileSize, throwOnConflict) { }
        internal PersistentQueueImpl(string path, int maxFileSize, bool throwOnConflict, bool isAsyncMode) : base(path, maxFileSize, throwOnConflict, isAsyncMode) { }

        public new static async Task<IPersistentQueueImpl<T>> CreateAsync(string path, CancellationToken cancellationToken = default)
        {
            var queue = new PersistentQueueImpl<T>(path, Constants._32Megabytes, true, true);
            await queue.InitializeAsync(cancellationToken);
            return queue;
        }

        new async Task InitializeAsync(CancellationToken cancellationToken)
        {
            using (await _configLockAsync.LockAsync(cancellationToken).ConfigureAwait(false))
            {
                _disposed = true;
                await base.LockAndReadQueueAsync(cancellationToken);
                _disposed = !_disposed;
            }
        }

        public new IPersistentQueueSession<T> OpenSession()
        {
            return new PersistentQueueSession<T>(this, CreateWriter(), SuggestedWriteBuffer, FileTimeoutMilliseconds);
        }

        public new async Task<IPersistentQueueSession<T>> OpenSessionAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new PersistentQueueSession<T>(this, await CreateWriterAsync(cancellationToken).ConfigureAwait(false), SuggestedWriteBuffer, FileTimeoutMilliseconds);
        }
    }
}