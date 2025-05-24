namespace ModernDiskQueue.Implementation
{
    /// <summary>
    /// Used to set the serialization strategy for the queue
    /// </summary>
    public enum SerializationStrategy
    {
        /// <summary>
        /// XML serialization strategy
        /// </summary>
        Xml,
        /// <summary>
        /// JSON serialization strategy
        /// </summary>
        Json
    }
}