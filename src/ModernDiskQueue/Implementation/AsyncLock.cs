
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
        private volatile int _waiters = 0;
        private readonly SemaphoreSlim _semaphore = new(1, 1);
        private readonly AsyncLocal<(int recursionCount, int taskId)> _ownerInfo = new();
        private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Asynchronously acquires the lock. The returned IDisposable releases the lock when disposed.
        /// </summary>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation, with an IDisposable that releases the lock.</returns>
        public async Task<IDisposable> LockAsync(CancellationToken cancellationToken = default)
        {
            // Reentrant lock fast path
            var currentTaskId = Environment.CurrentManagedThreadId;
            var ownerInfo = _ownerInfo.Value;

            if (ownerInfo.taskId == currentTaskId && ownerInfo.recursionCount > 0)
            {
                // Already owned by this task, increment recursion count
                _ownerInfo.Value = (ownerInfo.recursionCount + 1, currentTaskId);
                return new Releaser(this, true);
            }

            // Fast path for uncontended case
            if (Interlocked.CompareExchange(ref _waiters, 1, 0) == 0 &&
                _semaphore.CurrentCount > 0 &&
                _semaphore.Wait(0, cancellationToken))
            {
                _ownerInfo.Value = (1, currentTaskId);
                return new Releaser(this, false);
            }

            // The contentious path
            Interlocked.Increment(ref _waiters);
            try
            {
                const int MaxRetries = 3;
                for (int i = 0; i < MaxRetries; i++)
                {
                    if (await _semaphore.WaitAsync(DefaultTimeout, cancellationToken).ConfigureAwait(false))
                    {
                        _ownerInfo.Value = (1, currentTaskId);
                        return new Releaser(this, false);
                    }
                }
                throw new TimeoutException($"Failed to acquire lock after {MaxRetries} attempts.");
            }
            finally
            {
                Interlocked.Decrement(ref _waiters);
            }
        }

        private void Release(bool isReentrant)
        {
            if (isReentrant)
            {
                // Decrease recursion count
                var info = _ownerInfo.Value;
                _ownerInfo.Value = (info.recursionCount - 1, info.taskId);
            }
            else
            {
                // Release the lock
                _ownerInfo.Value = default;
                _semaphore.Release();
            }
        }

        private class Releaser : IDisposable
        {
            private readonly AsyncLock _lock;
            private readonly bool _isReentrant;

            public Releaser(AsyncLock @lock, bool isReentrant)
            {
                _lock = @lock;
                _isReentrant = isReentrant;
            }

            public void Dispose()
            {
                _lock.Release(_isReentrant);
            }
        }
    }
}