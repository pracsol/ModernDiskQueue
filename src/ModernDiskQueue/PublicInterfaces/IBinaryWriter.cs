using System;

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
        /// Truncate the file
        /// </summary>
        void Truncate();
    }
}