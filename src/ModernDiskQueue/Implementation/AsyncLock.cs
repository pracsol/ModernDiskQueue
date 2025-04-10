
namespace ModernDiskQueue.Implementation
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Provides an async-compatible locking mechanism that can be used in place of standard lock statements.
    /// </summary>
    internal sealed class AsyncLock
    {
        private readonly SemaphoreSlim _semaphore = new(1, 1);
        private readonly Task<IDisposable> _releaser;

        /// <summary>
        /// Initializes a new instance of the AsyncLock class.
        /// </summary>
        public AsyncLock()
        {
            _releaser = Task.FromResult((IDisposable)new Releaser(this));
        }

        /// <summary>
        /// Asynchronously acquires the lock. The returned IDisposable releases the lock when disposed.
        /// </summary>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation, with an IDisposable that releases the lock.</returns>
        public async Task<IDisposable> LockAsync(CancellationToken cancellationToken = default)
        {
            // Fast path if semaphore is available
            if (_semaphore.CurrentCount > 0 &&
                await _semaphore.WaitAsync(0, cancellationToken).ConfigureAwait(false))
            {
                return _releaser.Result;
            }

            // Slower path with explicit await
            await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            return _releaser.Result;
        }

        /// <summary>
        /// Asynchronously acquires the lock before the provided timeout expires. The returned IDisposable releases the lock when disposed.
        /// </summary>
        /// <param name="timeout">The maximum time to wait for the lock.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation, with an IDisposable that releases the lock.</returns>
        public async Task<IDisposable?> TryLockAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
        {
            if (await _semaphore.WaitAsync(timeout, cancellationToken).ConfigureAwait(false))
            {
                return _releaser.Result;
            }
            return null;
        }

        public async Task<IDisposable> LockAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
        {
            // Fast path if semaphore is available
            if (_semaphore.CurrentCount > 0 &&
                await _semaphore.WaitAsync(0, cancellationToken).ConfigureAwait(false))
            {
                return _releaser.Result;
            }

            // Use the specified timeout
            if (await _semaphore.WaitAsync(timeout, cancellationToken).ConfigureAwait(false))
            {
                return _releaser.Result;
            }

            throw new TimeoutException($"Failed to acquire lock within {timeout.TotalSeconds} seconds.");
        }

        /// <summary>
        /// A disposable struct that releases the lock when disposed.
        /// </summary>
        private sealed class Releaser : IDisposable, IAsyncDisposable
        {
            private readonly AsyncLock _toRelease;

            internal Releaser(AsyncLock toRelease)
            {
                _toRelease = toRelease;
            }

            public void Dispose()
            {
                _toRelease._semaphore.Release();
            }

            public ValueTask DisposeAsync()
            {
                _toRelease._semaphore.Release();
                return ValueTask.CompletedTask;
            }
        }
    }
}