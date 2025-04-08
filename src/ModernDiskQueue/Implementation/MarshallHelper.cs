namespace ModernDiskQueue.Implementation
{
    using System;
    using System.Runtime.CompilerServices;
    using System.Runtime.InteropServices;

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
        [SkipLocalsInit]
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
        /// <exception cref="ArgumentNullException">Returned when data is null.</exception>"
        [SkipLocalsInit]
        public static T Deserialize<T>(byte[] data) where T : struct
        {
            ArgumentNullException.ThrowIfNull(data);

            var structSize = Marshal.SizeOf<T>();

            // If we have complete data, use the fast path
            if (data.Length >= structSize)
            {
                return MemoryMarshal.Read<T>(data);
            }

            // Slow path for partial data
            return DeserializePartial<T>(data, structSize);
        }

        /// <summary>
        /// Deserializes a structure from a ReadOnlySpan - faster alternative when data is already in a span.
        /// </summary>
        /// <param name="data">The span containing binary data to deserialize.</param>
        /// <returns>The deserialized structure.</returns>
        [SkipLocalsInit]
        public static T Deserialize<T>(ReadOnlySpan<byte> data) where T : struct
        {
            var structSize = Marshal.SizeOf<T>();

            // Fast path for complete data
            if (data.Length >= structSize)
            {
                return MemoryMarshal.Read<T>(data);
            }

            // Convert to array for partial data path
            return DeserializePartial<T>(data.ToArray(), structSize);
        }

        /// <summary>
        /// Helper for handling partial data deserialization.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static T DeserializePartial<T>(byte[] data, int structSize) where T : struct
        {
            var pointer = IntPtr.Zero;
            try
            {
                pointer = Marshal.AllocHGlobal(structSize);
                // Zero out the memory first
                unsafe
                {
                    Unsafe.InitBlockUnaligned((void*)pointer, 0, (uint)structSize);
                }
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
