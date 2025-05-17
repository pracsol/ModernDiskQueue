using System.Threading;
using System.Threading.Tasks;

namespace ModernDiskQueue.Implementation.Interfaces
{
    /// <inheritdoc/>
    // ReSharper disable once UnusedTypeParameter
    public interface IPersistentQueueImpl<T> : IPersistentQueueImpl
    {
        /// <summary>
        /// Asynchronously lock the queue for use, and give access to session methods.
        /// The session <b>MUST</b> be disposed as soon as possible.
        /// </summary>
        /// <param name="cancellationToken">Token to monitor for cancellation requests.</param>
        /// <returns>A task that represents the asynchronous operation with a session that can be used to interact with the <see cref="IPersistentQueueImpl{T}"/> queue</returns>
        new Task<IPersistentQueueSession<T>> OpenSessionAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Asynchronously lock the queue for use, and give access to session methods.
        /// The session <b>MUST</b> be disposed as soon as possible.
        /// </summary>
        /// <param name="serializationStrategy">Specify a custom serialization strategy.</param>
        /// <param name="cancellationToken">Token to monitor for cancellation requests.</param>
        /// <returns></returns>
        Task<IPersistentQueueSession<T>> OpenSessionAsync(ISerializationStrategy<T> serializationStrategy, CancellationToken cancellationToken = default);
    }
}
