using System.Threading;
using System.Threading.Tasks;

namespace ModernDiskQueue
{
    /// <summary>
    /// This class performs basic binary serialization from objects of Type T to byte arrays suitable for use in DiskQueue sessions.
    /// </summary>
    /// <remarks>
    /// If you don't want to define both sync and async (de)serialization methods, consider using <see cref="AsyncSerializationStrategyBase{T}"/> or <see cref="SyncSerializationStrategyBase{T}"/>.
    /// </remarks>
    public interface ISerializationStrategy<T>
    {
        /// <summary>
        /// Asynchronously deserializes byte array into object reference of type T.
        /// </summary>
        /// <param name="bytes">Byte array to deserialize</param>
        /// <param name="cancellationToken">Token to monitor for cancellation requests</param>
        /// <returns>A task that represents the asynchronous operation with the deserialized object</returns>
        public ValueTask<T?> DeserializeAsync(byte[]? bytes, CancellationToken cancellationToken = default);

        /// <summary>
        /// Asynchronously serializes passed object into byte array suitable for queuing.
        /// </summary>
        /// <param name="obj">Object to serialize. Class must be decorated with Serializable annotation.</param>
        /// <param name="cancellationToken">Token to monitor for cancellation requests</param>
        /// <returns>A task that represents the asynchronous operation with the serialized byte array</returns>
        public ValueTask<byte[]?> SerializeAsync(T? obj, CancellationToken cancellationToken = default);

        /// <summary>
        /// Serialized passed object into byte array suitable for queuing into a <see cref="PersistentQueue{T}"/>.
        /// </summary>
        /// <param name="obj">Object to serialize. Class must be decorated with Serializable annotation.</param>
        /// <returns>Byte array.</returns>
        public byte[]? Serialize(T? obj);

        /// <summary>
        /// Deserializes byte array into object reference of type T.
        /// </summary>
        /// <param name="bytes">Byte array to deserialize</param>
        /// <returns>Object instance of type T.</returns>
        public T? Deserialize(byte[]? bytes);
    }
}