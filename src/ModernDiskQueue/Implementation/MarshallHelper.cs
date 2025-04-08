using System;
using System.Runtime.InteropServices;

namespace ModernDiskQueue.Implementation
{
    /// <summary>
    /// Helper class used to help with serializing to bytes
    /// </summary>
    public static class MarshallHelper
    {
        /// <summary>
        /// Serializes a structure to a byte array.
        /// </summary>
        /// <param name="value">The structure to serialize to a byte array.</param>
        /// <returns>The object serialized to a byte array, ready to write to a stream.</returns>
        public static byte[] Serialize<T>(T value) where T : struct
        {
            // Get the size of our structure in bytes
            var size = Marshal.SizeOf<T>();
            var bytes = new byte[size];

            // Use direct memory write
            MemoryMarshal.Write(bytes.AsSpan(), in value);

            return bytes;
        }

        /// <summary>
        /// Deserializes a structure from a byte array.
        /// </summary>
        /// <param name="data">The object binary data to deserialize.</param>
        /// <returns>The deserialized structure.</returns>
        public static T Deserialize<T>(byte[] data) where T : struct
        {
            var structSize = Marshal.SizeOf<T>();

            // If we have complete data, use the fast path
            if (data.Length >= structSize)
            {
                var span = MemoryMarshal.Cast<byte, T>(data.AsSpan());
                return span[0];
            }

            // Slow path for partial data
            var pointer = IntPtr.Zero;
            try
            {
                pointer = Marshal.AllocHGlobal(structSize);
                Marshal.Copy(new byte[structSize], 0, pointer, structSize);
                Marshal.Copy(data, 0, pointer, Math.Min(data.Length, structSize));
                return Marshal.PtrToStructure<T>(pointer);
            }
            finally
            {
                if (pointer != IntPtr.Zero)
                {
                    Marshal.FreeHGlobal(pointer);
                }
            }
        }
    }
}
