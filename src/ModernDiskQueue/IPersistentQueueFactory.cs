
namespace ModernDiskQueue
{
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Factory interface for creating <see cref="PersistentQueue{T}"/> instances.
    /// </summary>
    public interface IPersistentQueueFactory
    {
        /// <summary>
        /// Create a new queue instance.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="storagePath"></param>
        /// <returns></returns>
        PersistentQueue<T> Create<T>(string storagePath);

        /// <summary>
        /// Create a new queue instance.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="storagePath"></param>
        /// <param name="maxSize"></param>
        /// <param name="throwOnConflict"></param>
        /// <returns></returns>
        PersistentQueue<T> Create<T>(string storagePath, int maxSize, bool throwOnConflict = true);

        /// <summary>
        /// Create a new queue instance.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="storagePath"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task<PersistentQueue<T>> CreateAsync<T>(string storagePath, CancellationToken cancellationToken = default);

        /// <summary>
        /// Create a new queue instance.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="storagePath"></param>
        /// <param name="maxSize"></param>
        /// <param name="throwOnConflict"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task<PersistentQueue<T>> CreateAsync<T>(string storagePath, int maxSize, bool throwOnConflict = true, CancellationToken cancellationToken = default);
    }
}
