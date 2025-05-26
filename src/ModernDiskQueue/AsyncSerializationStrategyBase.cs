using System.Threading.Tasks;
using System.Threading;
using System;

namespace ModernDiskQueue
{
    /// <summary>
    /// This abstract class allows implementation of <see cref="ISerializationStrategy{T}"/> without having to define the synchronous (de)serialize methods.
    /// <para>Using the default sync implementations is not recommended, as they block the calling thread.</para>
    /// </summary>
    /// <remarks>
    /// Default implementations of <see cref="Serialize"/> and <see cref="Deserialize"/> methods are provided, which call the asynchronous versions of these methods wrapped in GetAwaiter().GetResult().
    /// </remarks>
    public abstract class AsyncSerializationStrategyBase<T> : ISerializationStrategy<T>
    {
        /// <inheritdoc/>
        public abstract ValueTask<byte[]?> SerializeAsync(T? obj, CancellationToken cancellationToken = default);

        /// <inheritdoc/>
        public abstract ValueTask<T?> DeserializeAsync(byte[]? bytes, CancellationToken cancellationToken = default);

        /// <inheritdoc/>
        public virtual byte[]? Serialize(T? obj)
        {
            try
            {
                return SerializeAsync(obj).AsTask().GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Error in synchronous fallback for async serialization", ex);
            }
        }

        /// <inheritdoc/>
        public virtual T? Deserialize(byte[]? bytes)
        {
            try
            {
                return DeserializeAsync(bytes).AsTask().GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Error in synchronous fallback for async deserialization", ex);
            }
        }
    }
}