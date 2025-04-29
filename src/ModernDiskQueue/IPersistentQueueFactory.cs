
namespace ModernDiskQueue
{
    using System;
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

        /// <summary>
        /// Create a new queue instance.
        /// </summary>
        Task<PersistentQueue> CreateAsync(string storagePath, int maxSize, bool throwOnConflict = true, CancellationToken cancellationToken = default);

        /// <summary>
        /// Create a new queue instance.
        /// </summary>
        Task<PersistentQueue> CreateAsync(string storagePath, CancellationToken cancellationToken = default);

        /// <summary>
        /// Wait for a queue to be created.
        /// </summary>
        Task<IPersistentQueue> WaitForAsync(string storagePath, TimeSpan maxWait, CancellationToken cancellationToken = default);

        /// <summary>
        /// Wait for a queue to be created.
        /// </summary>
        Task<IPersistentQueue> WaitForAsync(string storagePath, int maxSize, bool throwOnConflict, TimeSpan maxWait, CancellationToken cancellationToken = default);

        /// <summary>
        /// Wait for a queue to be created.
        /// </summary>
        Task<IPersistentQueue<T>> WaitForAsync<T>(string storagePath, TimeSpan maxWait, CancellationToken cancellationToken = default);

        /// <summary>
        /// Wait for a queue to be created.
        /// </summary>
        Task<IPersistentQueue<T>> WaitForAsync<T>(string storagePath, int maxSize, bool throwOnConflict, TimeSpan maxWait, CancellationToken cancellationToken = default);
    }
}
