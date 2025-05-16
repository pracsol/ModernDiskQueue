using System;
using System.Threading;
using System.Threading.Tasks;

namespace ModernDiskQueue.PublicInterfaces
{
    /// <summary>
    /// Wrapper around BinaryWriter
    /// </summary>
    public interface IBinaryWriter : IDisposable
    {
        /// <summary>
        /// Write bytes to the current position
        /// </summary>
        long Write(byte[] bytes);

        /// <summary>
        /// Write all bytes to a stream, returning new position
        /// </summary>
        ValueTask<long> WriteAsync(byte[] bytes, CancellationToken cancellationToken = default);

        /// <summary>
        /// Write all bytes to a stream, returning new position
        /// </summary>
        ValueTask<long> WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken cancellationToken = default);

        /// <summary>
        /// Truncate the file
        /// </summary>
        void Truncate();
    }
}