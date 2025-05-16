using System;

namespace ModernDiskQueue.Implementation.Interfaces
{
    /// <summary>
    /// Wrapper around BinaryReader
    /// </summary>
    public interface IBinaryReader : IDisposable
    {
        /// <summary>
        /// Read an Int32 from the current stream position
        /// </summary>
        int ReadInt32();

        /// <summary>
        /// Read a byte from the current stream position
        /// </summary>
        byte ReadByte();

        /// <summary>
        /// Read a number of bytes
        /// </summary>
        byte[] ReadBytes(int count);

        /// <summary>
        /// Read length of underlying stream
        /// </summary>
        long GetLength();

        /// <summary>
        /// Get current position in underlying stream
        /// </summary>
        long GetPosition();

        /// <summary>
        /// Read an Int64 from the current stream position
        /// </summary>
        long ReadInt64();
    }
}