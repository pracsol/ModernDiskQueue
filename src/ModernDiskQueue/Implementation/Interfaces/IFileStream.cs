namespace ModernDiskQueue.Implementation.Interfaces
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Wrapper for file activity
    /// </summary>
    public interface IFileStream : IDisposable, IAsyncDisposable
    {
        /// <summary>
        /// Write all bytes to a stream, returning new position
        /// </summary>
        long Write(byte[] bytes);

        /// <summary>
        /// Write all bytes to a stream, returning new position
        /// </summary>
        ValueTask<long> WriteAsync(byte[] bytes);

        /// <summary>
        /// Write all bytes to a stream, returning new position
        /// </summary>
        ValueTask<long> WriteAsync(byte[] bytes, CancellationToken cancellationToken = default);

        /// <summary>
        /// Write all bytes to a stream, returning new position
        /// </summary>
        ValueTask<long> WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken cancellationToken = default);

        /// <summary>
        /// Flush bytes from buffers to storage
        /// </summary>
        void Flush();

        /// <summary>
        /// Asynchronously flush bytes from buffers to storage
        /// </summary>
        ValueTask FlushAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Move to a byte offset from the start of the stream
        /// </summary>
        void MoveTo(long offset);

        /// <summary>
        /// Read from stream into buffer, returning number of bytes actually read.
        /// If the underlying stream supplies no bytes, this adaptor should try until a timeout is reached.
        /// An exception will be thrown if the file returns no bytes within the timeout window.
        /// </summary>
        int Read(byte[] buffer, int offset, int length); //"End of file reached while trying to read queue item"

        /// <summary>
        /// Asynchronously read from stream into buffer, returning number of bytes actually read.
        /// </summary>
        ValueTask<int> ReadAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken = default);

        /// <summary>
        /// Return a binary reader for the given file stream
        /// </summary>
        /// <returns></returns>
        IBinaryReader GetBinaryReader();

        /// <summary>
        /// Extend the underlying stream to the given length
        /// </summary>
        void SetLength(long length);

        /// <summary>
        /// Asynchronously extend the underlying stream to the given length
        /// </summary>
        ValueTask SetLengthAsync(long length, CancellationToken cancellationToken = default);

        /// <summary>
        /// Set the read/write position of the underlying file
        /// </summary>
        void SetPosition(long position);

        /// <summary>
        /// Get the current read/write position of the underlying file
        /// </summary>
        long GetPosition();
    }
}