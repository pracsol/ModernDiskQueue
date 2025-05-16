namespace ModernDiskQueue.PublicInterfaces
{
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// This class performs basic binary serialization from objects of Type T to byte arrays suitable for use in ModernDiskQueue sessions.
    /// </summary>
    public interface IAsyncSerializationStrategy<T>
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
    }
}