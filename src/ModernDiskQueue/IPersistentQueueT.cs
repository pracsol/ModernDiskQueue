using System.Threading;
using System.Threading.Tasks;
using ModernDiskQueue.Implementation;

namespace ModernDiskQueue
{
    /// <inheritdoc />
    public interface IPersistentQueue<T> : IPersistentQueue
    {
        /// <summary>
        /// Open a read/write session
        /// </summary>
        new IPersistentQueueSession<T> OpenSession();

        /// <summary>
        /// Open a read/write session asynchronously.
        /// </summary>
        /// <param name="cancellationToken"><see cref="CancellationToken"/>.</param>
        /// <returns><see cref="IPersistentQueueSession{T}"/></returns>
        new Task<IPersistentQueueSession<T>> OpenSessionAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Open a read/write session asynchronously.
        /// </summary>
        /// <param name="serializationStrategy">Specify a custom serialization strategy using <see cref="ISerializationStrategy{T}"/>.</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/>.</param>
        /// <returns><see cref="IPersistentQueueSession{T}"/></returns>
        Task<IPersistentQueueSession<T>> OpenSessionAsync(ISerializationStrategy<T> serializationStrategy, CancellationToken cancellationToken = default);

        /// <summary>
        /// Open a read/write session asynchronously.
        /// </summary>
        /// <param name="serializationStrategy">Specify a custom serialization strategy using <see cref="SerializationStrategy"/></param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/>.</param>
        /// <returns><see cref="IPersistentQueueSession{T}"/></returns>
        Task<IPersistentQueueSession<T>> OpenSessionAsync(SerializationStrategy serializationStrategy, CancellationToken cancellationToken = default);
    }
}