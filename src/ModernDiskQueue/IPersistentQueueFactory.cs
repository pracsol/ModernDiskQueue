
namespace ModernDiskQueue
{
    using ModernDiskQueue.Implementation;
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Factory interface for creating <see cref="IPersistentQueueFactory"/> instances.
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
        Task<IPersistentQueue<T>> CreateAsync<T>(string storagePath, CancellationToken cancellationToken = default);

        /// <summary>
        /// Create a new queue instance.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="storagePath"></param>
        /// <param name="maxSize"></param>
        /// <param name="throwOnConflict"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task<IPersistentQueue<T>> CreateAsync<T>(string storagePath, int maxSize, bool throwOnConflict = true, CancellationToken cancellationToken = default);

        /// <summary>
        /// Create a new queue instance.
        /// </summary>
        Task<IPersistentQueue> CreateAsync(string storagePath, int maxSize, bool throwOnConflict = true, CancellationToken cancellationToken = default);

        /// <summary>
        /// Create a new queue instance.
        /// </summary>
        Task<IPersistentQueue> CreateAsync(string storagePath, CancellationToken cancellationToken = default);

        /// <summary>
        /// Create a new queue instance.
        /// </summary>
        Task<IPersistentQueue<T>> CreateAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] T>(string storagePath, ISerializationStrategy<T> defaultSessionStrategy, CancellationToken cancellationToken = default);

        /// <summary>
        /// Create a new queue instance.
        /// </summary>
        Task<IPersistentQueue<T>> CreateAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] T>(string storagePath, SerializationStrategy defaultSessionStrategy, CancellationToken cancellationToken = default);

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

        /// <summary>
        /// Wait for a queue to be created.
        /// </summary>
        Task<IPersistentQueue<T>> WaitForAsync<T>(string storagePath, ISerializationStrategy<T> defaultSerializationStrategy, int maxSize, bool throwOnConflict, TimeSpan maxWait, CancellationToken cancellationToken = default);
    }
}
