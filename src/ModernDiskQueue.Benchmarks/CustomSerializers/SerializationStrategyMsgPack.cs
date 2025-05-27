// -----------------------------------------------------------------------
// <copyright file="SerializationStrategyMsgPack.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Benchmarks.CustomSerializers
{
    using System.Threading.Tasks;
    using MessagePack;

    /// <summary>
    /// Implements <see cref="ISerializationStrategy{T}"/> using MessagePack for serialization and deserialization.
    /// </summary>
    /// <typeparam name="T">Type of object that will be (de)serialized.</typeparam>
    public class SerializationStrategyMsgPack<T> : AsyncSerializationStrategyBase<T>
    {
        /// <inheritdoc/>
        public async override ValueTask<byte[]?> SerializeAsync(T? obj, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(obj);
            using MemoryStream ms = new();
            await MessagePackSerializer.SerializeAsync<T>(ms, obj, null, cancellationToken);
            return ms.ToArray();
        }

        /// <inheritdoc/>
        public async override ValueTask<T?> DeserializeAsync(byte[]? data, CancellationToken cancellationToken = default)
        {
            if (data == null)
            {
                return default;
            }

            using MemoryStream ms = new(data);
            return await MessagePackSerializer.DeserializeAsync<T>(ms, null, cancellationToken);
        }
    }
}