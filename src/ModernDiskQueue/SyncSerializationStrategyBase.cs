namespace ModernDiskQueue
{
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// This abstract class allows implementation of <see cref="ISerializationStrategy{T}"/> without having to define the asynchronous (de)serialize methods.
    /// </summary>
    /// <remarks>
    /// Default implementations of <see cref="SerializeAsync"/> and <see cref="DeserializeAsync"/> methods are provided, which call the synchronous versions of the methods wrapped in ValueTask return types.
    /// </remarks>
    public abstract class SyncSerializationStrategyBase<T> : ISerializationStrategy<T>
    {
        /// <inheritdoc/>
        public abstract byte[]? Serialize(T? obj);

        /// <inheritdoc/>
        public abstract T? Deserialize(byte[]? bytes);

        /// <inheritdoc/>
        public virtual ValueTask<T?> DeserializeAsync(byte[]? bytes, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new ValueTask<T?>(Deserialize(bytes));
        }

        /// <inheritdoc/>
        public virtual ValueTask<byte[]?> SerializeAsync(T? obj, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new ValueTask<byte[]?>(Serialize(obj));
        }
    }
}