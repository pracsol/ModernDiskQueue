
namespace ModernDiskQueue.Implementation
{
    using Microsoft.Extensions.Logging;
    using ModernDiskQueue.Implementation.Logging;
    using ModernDiskQueue.PublicInterfaces;
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Provides an async-compatible locking mechanism that can be used in place of standard lock statements.
    /// </summary>
    internal sealed class AsyncLock
    {
        private static readonly ILogger<AsyncLock> StaticLogger = LoggerAdapter.CreateLogger<AsyncLock>();
        private volatile int _waiters = 0;
        private readonly SemaphoreSlim _semaphore = new(1, 1);
        private readonly AsyncLocal<(int recursionCount, int taskId)> _ownerInfo = new();
        private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);

        public async Task<IDisposable> LockAsync(CancellationToken cancellationToken = default)
        {
            return await LockAsync(string.Empty, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Asynchronously acquires the lock. The returned IDisposable releases the lock when disposed.
        /// </summary>
        /// <param name="lockName">Optional name of lock, primarily used for tracing in logs.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation, with an IDisposable that releases the lock.</returns>
        public async Task<IDisposable> LockAsync(string lockName = "", CancellationToken cancellationToken = default)
        {
            var stopWatch = Stopwatch.StartNew();
            var currentTaskId = Environment.CurrentManagedThreadId;
            var ownerInfo = _ownerInfo.Value;

            StaticLogger.LogTrace("[AsyncLock] Attempting to acquire lock {LockName} by task {TaskId}", lockName, currentTaskId);

            // Reentrant lock fast path
            if (ownerInfo.taskId == currentTaskId && ownerInfo.recursionCount > 0)
            {
                // Already owned by this task, increment recursion count
                _ownerInfo.Value = (ownerInfo.recursionCount + 1, currentTaskId);
                StaticLogger.LogTrace("[AsyncLock] Reentrant lock acquired by task {TaskId} after {ElapsedMilliseconds}ms", currentTaskId, stopWatch.ElapsedMilliseconds);
                return new Releaser(this, true, currentTaskId, lockName);
            }

            // Fast path for uncontended case
            if (Interlocked.CompareExchange(ref _waiters, 1, 0) == 0 &&
                _semaphore.CurrentCount > 0 &&
                _semaphore.Wait(0, cancellationToken))
            {
                _ownerInfo.Value = (1, currentTaskId);
                StaticLogger.LogTrace("[AsyncLock] Lock acquired by task {TaskId} after {ElapsedMilliseconds}ms", currentTaskId, stopWatch.ElapsedMilliseconds);
                return new Releaser(this, false, currentTaskId, lockName);
            }

            // The contentious path
            Interlocked.Increment(ref _waiters);
            try
            {
                const int MaxRetries = 5;
                var delay = TimeSpan.FromMilliseconds(50); // Initial delay
                var random = new Random();
                for (int i = 0; i < MaxRetries; i++)
                {
                    if (await _semaphore.WaitAsync(DefaultTimeout, cancellationToken).ConfigureAwait(false))
                    {
                        _ownerInfo.Value = (1, currentTaskId);
                        StaticLogger.LogTrace("[AsyncLock] Lock acquired by task {TaskId} after {ElapsedMilliseconds}ms and {RetryCount} of {MaxRetries} retries.", currentTaskId, stopWatch.ElapsedMilliseconds, i, MaxRetries);
                        return new Releaser(this, false, currentTaskId, lockName);
                    }

                    // Exponential backoff with jitter
                    await Task.Delay(delay + TimeSpan.FromMilliseconds(random.Next(10, 100)), cancellationToken).ConfigureAwait(false);
                    delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, 1000)); // Cap delay at 1 second
                }

                throw new TimeoutException($"[AsyncLock] Failed to acquire lock after {MaxRetries} attempts.");
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
            private static readonly ILogger<Releaser> StaticLogger = LoggerAdapter.CreateLogger<Releaser>();
            private readonly AsyncLock _lock;
            private readonly bool _isReentrant;
            private readonly int _creatorThreadId;
            private readonly string _lockName;

            public Releaser(AsyncLock @lock, bool isReentrant, int creatorThreadId, string lockName = "")
            {
                _lock = @lock;
                _isReentrant = isReentrant;
                _creatorThreadId = creatorThreadId;
                _lockName = lockName;
            }

            public void Dispose()
            {
                StaticLogger.LogTrace("[AsyncLock] Release lock {LockName} from thread {Creator} on Thread {CurrentManagedThreadId}", _lockName, _creatorThreadId, Environment.CurrentManagedThreadId);
                _lock.Release(_isReentrant);
            }
        }
    }
}