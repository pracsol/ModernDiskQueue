
namespace ModernDiskQueue.Implementation
{
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.Abstractions;
    using ModernDiskQueue.Implementation.Logging;
    using ModernDiskQueue.PublicInterfaces;
    using System;
    using System.Buffers;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Default persistent queue session.
    /// <para>You should use <see cref="IPersistentQueue.OpenSession"/> to get a session.</para>
    /// <example>using (var q = PersistentQueue.WaitFor("myQueue")) using (var session = q.OpenSession()) { ... }</example>
    /// </summary>
    public class PersistentQueueSession : IPersistentQueueSession
    {
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger<IPersistentQueueSession> _logger;
        private readonly List<Operation> _operations = [];
        private readonly List<Exception> _pendingWritesFailures = [];
        private readonly List<WaitHandle> _pendingWritesHandles = [];
        private readonly AsyncLock _pendingWritesFailuresLockAsync = new();
        private IFileStream _currentStream;
        private readonly int _writeBufferSize;
        private readonly int _timeoutLimitMilliseconds;
        private readonly IPersistentQueueImpl _queue;
        private readonly List<IFileStream> _streamsToDisposeOnFlush = [];
        private volatile bool _disposed;

        private readonly List<byte[]> _buffer = [];
        private int _bufferSize;

        private const int MinSizeThatMakeAsyncWritePractical = 64 * 1024;

        /// <summary>
        /// Create a default persistent queue session.
        /// <para>You should use <see cref="IPersistentQueue.OpenSession"/> to get a session.</para>
        /// <example>using (var q = PersistentQueue.WaitFor("myQueue")) using (var session = q.OpenSession()) { ... }</example>
        /// </summary>
        public PersistentQueueSession(ILoggerFactory loggerFactory, IPersistentQueueImpl queue, IFileStream currentStream, int writeBufferSize, int timeoutLimit)
        {
            _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            _logger = loggerFactory?.CreateLogger<PersistentQueueSession>() ?? NullLogger<PersistentQueueSession>.Instance;
            _queue = queue;
            _currentStream = currentStream;
            if (writeBufferSize < MinSizeThatMakeAsyncWritePractical)
                writeBufferSize = MinSizeThatMakeAsyncWritePractical;
            _writeBufferSize = writeBufferSize;
            _timeoutLimitMilliseconds = timeoutLimit;
            _disposed = false;
        }

        /// <summary>
        /// Queue data for a later decode. Data is written on `Flush()`
        /// </summary>
        public void Enqueue(byte[] data)
        {
            _buffer.Add(data);
            _bufferSize += data.Length;

            // When the data size exceeds the write buffer size, we need to go ahead and flush the buffer
            // by committing the operation to the queue
            // This will write the data to the stream and collect any exceptions in the _pendingWriteExceptions collection.
            if (_bufferSize > _writeBufferSize)
            {
                AsyncFlushBuffer();
            }
        }

        /// <summary>
        /// Asynchronously queue data for a later decode. Data is written on `FlushAsync()`
        /// </summary>
        public async ValueTask EnqueueAsync(byte[] data, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _logger.LogTrace("[PQS] Enqueu started on thread {CurrentThread}.", Environment.CurrentManagedThreadId);
            _buffer.Add(data);
            _bufferSize += data.Length;

            if (_bufferSize > _writeBufferSize)
            {
                await FlushBufferAsync(cancellationToken).ConfigureAwait(false);
            }
            _logger.LogTrace("[PQS] Enqueue completed on thread {CurrentThread}.", Environment.CurrentManagedThreadId);
        }

        /// <summary>
        /// Try to pull data from the queue. Data is removed from the queue on `Flush()`
        /// </summary>
        public byte[]? Dequeue()
        {
            var entry = _queue.Dequeue();
            if (entry == null)
                return null;
            _operations.Add(new Operation(
                OperationType.Dequeue,
                entry.FileNumber,
                entry.Start,
                entry.Length
            ));
            return entry.Data;
        }

        /// <summary>
        /// Asynchronously dequeue data from the queue. Data is removed from the queue on `FlushAsync()`
        /// </summary>
        public async ValueTask<byte[]?> DequeueAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                _logger.LogTrace("[PQS] Dequeue started on thread {CurrentThread}.", Environment.CurrentManagedThreadId);
                // Use the async dequeue method in the queue implementation if available
                Entry? entry = await _queue.DequeueAsync(cancellationToken).ConfigureAwait(false);

                if (entry == null)
                    return null;

                _operations.Add(new Operation(
                    OperationType.Dequeue,
                    entry.FileNumber,
                    entry.Start,
                    entry.Length
                ));

                return entry.Data;
            }
            finally
            {
                _logger.LogTrace("[PQS] Dequeue completed on thread {CurrentThread}.", Environment.CurrentManagedThreadId);
            }
        }

        /// <summary>
        /// This method is used to flush the buffer to the stream. It is called when the buffer size exceeds the specified limit during the Enqueue operation.
        /// Unlike FlushBufferSync, this method swallows any write exceptions, collecting them into _pendingWritesFailures
        /// which will get thrown as an AggregateException at the end of the Flush operation.
        /// </summary>
        /// <remarks>
        /// This is a legacy function (from original DQ implementation) using the async prefix, not part of new async work in MDQ.
        /// It calls an async write operation (AsyncWriteToStream) but returns immediately, and
        /// the manualresetevents and exception collection are used to let these operations complete in their own time.
        /// </remarks>>
        private void AsyncFlushBuffer()
        {
            // AcquireWriter will roll over to a new file if size has been exceeded after the write operation.
            _queue.AcquireWriter(_currentStream, AsyncWriteToStream, OnReplaceStream);
        }

        private void FlushBufferSync()
        {
            _queue.AcquireWriter(_currentStream, stream =>
            {
                var data = ConcatenateBufferAndAddIndividualOperations(stream);
                // This will throw an exception immediately if there's an issue.
                return Task.FromResult(stream.Write(data));
            }, OnReplaceStream);
        }

        /// <summary>
        /// This is the async version of FlushBufferSync.
        /// </summary>
        private async Task FlushBufferAsync(CancellationToken cancellationToken = default)
        {
            await _queue.AcquireWriterAsync(
                _currentStream,
                WriteToStreamAsync,
                OnReplaceStream,
                cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Asynchronously write to the stream. This method is called when the buffer size exceeds the specified limit.
        /// This method is called by AsyncFlushBuffer, which is called by Enqueue when the buffer size exceeds the buffer size limit
        /// specified during session instantiation.
        /// Exceptions are collected in _pendingWritesFailures, which will be thrown as an AggregateException at the end of the Flush operation.
        /// </summary>
        private async Task<long> AsyncWriteToStream(IFileStream stream)
        {
            // This will collection exceptions in _pendingWriteFailures, which 
            // will get thrown as an AggregateException at end of Flush operation.
            var data = ConcatenateBufferAndAddIndividualOperations(stream);
            var resetEvent = new ManualResetEvent(false);
            _pendingWritesHandles.Add(resetEvent);
            var positionAfterWrite = stream.GetPosition() + data.Length;
            try
            {
                positionAfterWrite = await stream.WriteAsync(data).ConfigureAwait(false);
                resetEvent.Set();
            }
            catch (Exception e)
            {
                lock (_pendingWritesFailures)
                {
                    _pendingWritesFailures.Add(e);
                    resetEvent.Set();
                }
            }

            return positionAfterWrite;
        }

        private async Task<long> WriteToStreamAsync(IFileStream stream, CancellationToken cancellationToken = default)
        {
            // Use of ManualResetEvents and _pendingWritesFailures has been dropped
            // in the async model. I believe those design decisions were made to
            // support the use of asyncwrite in a synchronous context, and so
            // are not needed anymore.
            var data = ConcatenateBufferAndAddIndividualOperations(stream);
            return await stream.WriteAsync(data.AsMemory(), cancellationToken).ConfigureAwait(false);
        }

        private byte[] ConcatenateBufferAndAddIndividualOperations(IFileStream stream)
        {
            byte[] rentedBuffer = ArrayPool<byte>.Shared.Rent(_bufferSize);
            try
            {
                var start = (int)stream.GetPosition();
                var index = 0;

                foreach (var bytes in _buffer)
                {
                    _operations.Add(new Operation(
                        OperationType.Enqueue,
                        _queue.CurrentFileNumber,
                        start,
                        bytes.Length
                    ));
                    Buffer.BlockCopy(bytes, 0, rentedBuffer, index, bytes.Length);
                    start += bytes.Length;
                    index += bytes.Length;
                }
                _bufferSize = 0;
                _buffer.Clear();

                byte[] result = new byte[index];
                Buffer.BlockCopy(rentedBuffer, 0, result, 0, index);
                return result;
            }
            finally
            {
                // It's a rental, return it!
                ArrayPool<byte>.Shared.Return(rentedBuffer, clearArray: false);
            }
        }

        private void OnReplaceStream(IFileStream newStream)
        {
            _streamsToDisposeOnFlush.Add(_currentStream);
            _currentStream = newStream;
        }

        /// <summary>
        /// Commit actions taken in this session since last flush.
        /// If the session is disposed with no flush, actions are not persisted
        /// to the queue (Enqueues are not written, dequeues are left on the queue)
        /// </summary>
        public void Flush()
        {
            var fails = new List<Exception>();
            WaitForPendingWrites(fails);

            try
            {
                FlushBufferSync();
            }
            finally
            {
                foreach (var stream in _streamsToDisposeOnFlush)
                {
                    stream.Flush();
                    stream.Dispose();
                }

                _streamsToDisposeOnFlush.Clear();
            }

            if (fails.Count > 0)
            {
                throw new AggregateException(fails);
            }

            _currentStream.Flush();
            _queue.CommitTransaction(_operations);
            _operations.Clear();
        }

        /// <summary>
        /// Asynchronously commit actions taken in this session.
        /// If the session is disposed with no flush, actions are not persisted
        /// to the queue (Enqueues are not written, dequeues are left on the queue)
        /// </summary>
        public async Task FlushAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            _logger.LogTrace("[PQS] FlushAsync started on thread {CurrentThread}.", Environment.CurrentManagedThreadId);

            var fails = new List<Exception>();
            // I don't think I need this method since we're not using the ManualResetEvents
            // that were used by the sync code.
            await WaitForPendingWritesAsync(fails, cancellationToken).ConfigureAwait(false);

            try
            {
                await FlushBufferAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                foreach (var stream in _streamsToDisposeOnFlush)
                {
                    try
                    {
                        await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
                        await stream.DisposeAsync().ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        fails.Add(ex);
                    }
                }

                _streamsToDisposeOnFlush.Clear();
            }

            if (fails.Count > 0)
            {
                throw new AggregateException(fails);
            }

            await _currentStream.FlushAsync(cancellationToken).ConfigureAwait(false);

            // Commit transactions using async if available
            await _queue.CommitTransactionAsync(_operations, cancellationToken).ConfigureAwait(false);
            _operations.Clear();
            _logger.LogTrace("[PQS] FlushAsync completed on thread {CurrentThread}.", Environment.CurrentManagedThreadId);
        }

        private void WaitForPendingWrites(List<Exception> exceptions)
        {
            var timeoutCount = 0;
            var total = _pendingWritesHandles.Count;
            while (_pendingWritesHandles.Count != 0)
            {
                var handles = _pendingWritesHandles.Take(32).ToArray();
                foreach (var handle in handles)
                {
                    _pendingWritesHandles.Remove(handle);
                }

                var ok = WaitHandle.WaitAll(handles, _timeoutLimitMilliseconds);
                if (!ok) timeoutCount++;

                foreach (var handle in handles)
                {
                    try
                    {
                        handle.Close();   // virtual
                        handle.Dispose(); // always base class
                    }
                    catch {/* ignore */ }
                }
            }
            AssertNoPendingWritesFailures(exceptions);
            if (timeoutCount > 0) exceptions.Add(new Exception($"File system async operations are timing out: {timeoutCount} of {total}"));
        }

        // Helper method for async waiting
        private async Task WaitForPendingWritesAsync(List<Exception> exceptions, CancellationToken cancellationToken)
        {
            // Create an AsyncLock instance for the waiting operation.
            AsyncLock waitLock = new();
            var timeoutCount = 0;
            var total = _pendingWritesHandles.Count;

            while (_pendingWritesHandles.Count != 0)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var handles = _pendingWritesHandles.Take(32).ToArray();
                foreach (var handle in handles)
                {
                    _pendingWritesHandles.Remove(handle);
                }

                using (await waitLock.LockAsync("waitlock", cancellationToken).ConfigureAwait(false))
                {
                    bool waitResult = false;

                    var timeoutTask = Task.Delay(_timeoutLimitMilliseconds, cancellationToken);

                    var handleTasks = handles.Select(h => WaitOneAsync(h, cancellationToken)).ToArray();

                    if (await Task.WhenAny(Task.WhenAll(handleTasks), timeoutTask).ConfigureAwait(false) == timeoutTask)
                    {
                        // timeout occurred.
                        timeoutCount++;
                    }
                    else
                    {
                        // All handles completed successfully
                        waitResult = true;
                    }

                    // Clean up handles
                    foreach (var handle in handles)
                    {
                        try
                        {
                            handle.Close();
                            handle.Dispose();
                        }
                        catch
                        {
                            // swallow exception.
                        }
                    }

                    if (!waitResult)
                    {
                        timeoutCount++;
                    }
                }
            }

            await AssertNoPendingWritesFailuresAsync(exceptions, cancellationToken).ConfigureAwait(false);

            if (timeoutCount > 0)
                exceptions.Add(new Exception($"File system async operations are timing out: {timeoutCount} of {total}"));
        }

        private void AssertNoPendingWritesFailures(List<Exception> exceptions)
        {
            lock (_pendingWritesFailures)
            {
                if (_pendingWritesFailures.Count == 0)
                    return;

                var array = _pendingWritesFailures.ToArray();
                _pendingWritesFailures.Clear();
                exceptions.Add(new PendingWriteException(array));
            }
        }

        private async Task AssertNoPendingWritesFailuresAsync(List<Exception> exceptions, CancellationToken cancellationToken)
        {
            using (await _pendingWritesFailuresLockAsync.LockAsync("waitlock", cancellationToken).ConfigureAwait(false))
            {
                if (_pendingWritesFailures.Count == 0)
                    return;

                var array = _pendingWritesFailures.ToArray();
                _pendingWritesFailures.Clear();
                exceptions.Add(new PendingWriteException(array));
            }
        }

        // Helper method to convert WaitHandle.WaitOne to async Task
        private Task WaitOneAsync(WaitHandle handle, CancellationToken cancellationToken)
        {
            // Short-circuit if already signaled
            if (handle.WaitOne(0))
            {
                return Task.CompletedTask;
            }

            var tcs = new TaskCompletionSource<bool>();

            // Register the handle for the wait
            var registerWaitHandle = ThreadPool.RegisterWaitForSingleObject(
                handle,
                (state, timedOut) => ((TaskCompletionSource<bool>)state!).TrySetResult(!timedOut),
                tcs,
                _timeoutLimitMilliseconds,
                true
            );

            // Link cancellation token
            if (cancellationToken.CanBeCanceled)
            {
                cancellationToken.Register(() => {
                    tcs.TrySetCanceled();
                    registerWaitHandle.Unregister(null);
                }, false);
            }

            return tcs.Task;
        }

        /// <summary>
        /// Close session, restoring any non-flushed operations
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _queue.Reinstate(_operations);
            _operations.Clear();
            foreach (var stream in _streamsToDisposeOnFlush)
            {
                stream.Dispose();
            }

            _currentStream.Dispose();
            GC.SuppressFinalize(this);
            Thread.Sleep(0);
        }

        /// <summary>
        /// Asynchronously disposes of the session, ensuring proper cleanup and data integrity
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            if (_disposed) return;
            _disposed = true;

            // Use async reinstate if available
            if (_operations.Count > 0)
            {
                try
                {
                    await _queue.ReinstateAsync(_operations).ConfigureAwait(false);
                }
                catch
                {
                    // Fall back to synchronous if async fails
                    _queue.Reinstate(_operations);
                }
            }

            _operations.Clear();

            // Clean up streams
            foreach (var stream in _streamsToDisposeOnFlush)
            {
                if (stream is IAsyncDisposable asyncDisposable)
                    await asyncDisposable.DisposeAsync().ConfigureAwait(false);
                else
                    stream.Dispose();
            }

            // Handle the current stream
            if (_currentStream is IAsyncDisposable asyncCurrentStream)
                await asyncCurrentStream.DisposeAsync().ConfigureAwait(false);
            else
                _currentStream.Dispose();

            GC.SuppressFinalize(this);
            _logger.LogTrace("[PQS] Disposing of session on thread {CurrentThread}", Environment.CurrentManagedThreadId);
        }

        /// <summary>
        /// Dispose queue on destructor. This is a safety-valve. You should ensure you
        /// dispose of sessions normally.
        /// </summary>
        ~PersistentQueueSession()
        {
            if (_disposed) return;
            Dispose();
        }
    }
}