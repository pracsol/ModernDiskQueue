namespace ModernDiskQueue
{
    /// <summary>
    /// This class performs basic binary serialization from objects of Type T to byte arrays suitable for use in DiskQueue sessions.
    /// </summary>
    public interface ISerializationStrategy<T> : ISyncSerializationStrategy<T>, IAsyncSerializationStrategy<T>
    {
    }
}