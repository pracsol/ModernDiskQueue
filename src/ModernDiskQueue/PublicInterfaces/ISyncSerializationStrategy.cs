namespace ModernDiskQueue.PublicInterfaces
{
    /// <summary>
    /// This class performs basic binary serialization from objects of Type T to byte arrays suitable for use in ModernDiskQueue sessions.
    /// </summary>
    public interface ISyncSerializationStrategy<T>
    {
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