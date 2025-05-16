using System.Threading;
using System.Threading.Tasks;

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
    }
}