using ModernDiskQueue.PublicInterfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ModernDiskQueue.Implementation
{
    /// <summary>
    /// Default persistent queue session.
    /// <para>You should use <see cref="IPersistentQueue.OpenSession"/> to get a session.</para>
    /// <example>using (var q = PersistentQueue.WaitFor("myQueue")) using (var session = q.OpenSession()) { ... }</example>
    /// </summary>
    public class PersistentQueueSession : IPersistentQueueSession
    {
        private readonly List<Operation> _operations = new();
        private readonly List<Exception> _pendingWritesFailures = new();
        private readonly List<WaitHandle> _pendingWritesHandles = new();
        private IFileStream _currentStream;
        private readonly int _writeBufferSize;
        private readonly int _timeoutLimitMilliseconds;
        private readonly IPersistentQueueImpl _queue;
        private readonly List<IFileStream> _streamsToDisposeOnFlush = new();
        private volatile bool _disposed;

        private readonly List<byte[]> _buffer = new();
        private int _bufferSize;

        private const int MinSizeThatMakeAsyncWritePractical = 64 * 1024;

        /// <summary>
        /// Create a default persistent queue session.
        /// <para>You should use <see cref="IPersistentQueue.OpenSession"/> to get a session.</para>
        /// <example>using (var q = PersistentQueue.WaitFor("myQueue")) using (var session = q.OpenSession()) { ... }</example>
        /// </summary>
        public PersistentQueueSession(IPersistentQueueImpl queue, IFileStream currentStream, int writeBufferSize, int timeoutLimit)
        {
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
            if (_bufferSize > _writeBufferSize)
            {
                AsyncFlushBuffer();
            }
        }

        /// <summary>
        /// Asynchronously queue data for a later decode. Data is written on `FlushAsync()`
        /// </summary>
        public async Task EnqueueAsync(byte[] data, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            _buffer.Add(data);
            _bufferSize += data.Length;

            if (_bufferSize > _writeBufferSize)
            {
                await FlushBufferAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// This is a legacy function using the async prefix. It calls an async write operation (AsyncWriteToStream) but since the method
        /// itself runs synchronously, it is not a true async method. It is kept for backward compatibility.
        /// </summary>
        private void AsyncFlushBuffer()
        {
            _queue.AcquireWriter(_currentStream, AsyncWriteToStream, OnReplaceStream);
        }

        // Private helper method for async buffer flushing
        private async Task FlushBufferAsync(CancellationToken cancellationToken = default)
        {
            await _queue.AcquireWriterAsync(
                _currentStream,
                stream => WriteToStreamAsync(stream, cancellationToken),
                OnReplaceStream,
                cancellationToken).ConfigureAwait(false);
        }

        private void SyncFlushBuffer()
        {
            _queue.AcquireWriter(_currentStream, stream =>
            {
                var data = ConcatenateBufferAndAddIndividualOperations(stream);
                return Task.FromResult(stream.Write(data));
            }, OnReplaceStream);
        }

        private async Task<long> AsyncWriteToStream(IFileStream stream)
        {
            var data = ConcatenateBufferAndAddIndividualOperations(stream);
            var resetEvent = new ManualResetEvent(false);
            _pendingWritesHandles.Add(resetEvent);
            var positionAfterWrite = stream.GetPosition() + data.Length;
            try
            {
                positionAfterWrite = await stream.WriteAsync(data);
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

        // Updated async write method with cancellation support
        private async Task<long> WriteToStreamAsync(IFileStream stream, CancellationToken cancellationToken)
        {
            var data = ConcatenateBufferAndAddIndividualOperations(stream);

            // Use TaskCompletionSource for better async pattern
            var tcs = new TaskCompletionSource<bool>();
            var resetEvent = new ManualResetEvent(false);
            _pendingWritesHandles.Add(resetEvent);

            var positionAfterWrite = stream.GetPosition() + data.Length;
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                positionAfterWrite = await stream.WriteAsync(data).ConfigureAwait(false);
                resetEvent.Set();
                tcs.TrySetResult(true);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                resetEvent.Set();
                tcs.TrySetCanceled(cancellationToken);
                throw;
            }
            catch (Exception e)
            {
                lock (_pendingWritesFailures)
                {
                    _pendingWritesFailures.Add(e);
                    resetEvent.Set();
                    tcs.TrySetException(e);
                }
            }

            return positionAfterWrite;
        }

        private byte[] ConcatenateBufferAndAddIndividualOperations(IFileStream stream)
        {
            var data = new byte[_bufferSize];
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
                Buffer.BlockCopy(bytes, 0, data, index, bytes.Length);
                start += bytes.Length;
                index += bytes.Length;
            }
            _bufferSize = 0;
            _buffer.Clear();
            return data;
        }

        private void OnReplaceStream(IFileStream newStream)
        {
            _streamsToDisposeOnFlush.Add(_currentStream);
            _currentStream = newStream;
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
        public async Task<byte[]?> DequeueAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

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
                SyncFlushBuffer();
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

            var fails = new List<Exception>();
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
                        // Try to use async flush if available
                        if (stream is IFileStream asyncStream)
                        {
                            await asyncStream.FlushAsync(cancellationToken).ConfigureAwait(false);
                        }
                        else
                        {
                            stream.Flush();
                        }

                        // Try to use async dispose if available
                        if (stream is IAsyncDisposable asyncDisposable)
                        {
                            await asyncDisposable.DisposeAsync().ConfigureAwait(false);
                        }
                        else
                        {
                            stream.Dispose();
                        }
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

            // Flush the current stream
            if (_currentStream is IFileStream asyncCurrentStream)
            {
                await asyncCurrentStream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                _currentStream.Flush();
            }

            // Commit transactions using async if available
            await _queue.CommitTransactionAsync(_operations, cancellationToken).ConfigureAwait(false);
            _operations.Clear();
        }

        // Helper method for async waiting
        private async Task WaitForPendingWritesAsync(List<Exception> exceptions, CancellationToken cancellationToken)
        {
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

                // Convert WaitAll to async task with timeout
                var waitTask = Task.Run(() => WaitHandle.WaitAll(handles, _timeoutLimitMilliseconds), cancellationToken);

                try
                {
                    // Wait for completion with cancellation support
                    var timeoutTask = Task.Delay(_timeoutLimitMilliseconds, cancellationToken);
                    var completedTask = await Task.WhenAny(waitTask, timeoutTask).ConfigureAwait(false);

                    if (completedTask == timeoutTask || !waitTask.Result)
                        timeoutCount++;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception)
                {
                    timeoutCount++;
                }

                // Clean up handles
                foreach (var handle in handles)
                {
                    try
                    {
                        handle.Close();
                        handle.Dispose();
                    }
                    catch {/* ignore */}
                }
            }

            AssertNoPendingWritesFailures(exceptions);

            if (timeoutCount > 0)
                exceptions.Add(new Exception($"File system async operations are timing out: {timeoutCount} of {total}"));
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