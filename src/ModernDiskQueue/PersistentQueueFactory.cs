
namespace ModernDiskQueue
{
    using ModernDiskQueue.Implementation;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using System.Threading.Tasks;
    /// <summary>
    /// Factory for creating <see cref="PersistentQueue{T}"/> instances.
    /// </summary>
    public class PersistentQueueFactory : IPersistentQueueFactory
    {
        /// <inheritdoc/>
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)]
        public PersistentQueue<T> Create<T>(string storagePath)
        {
            return new PersistentQueue<T>(storagePath);
        }

        /// <inheritdoc/>
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)]
        public PersistentQueue<T> Create<T>(string storagePath, int maxSize, bool throwOnConflict = true)
        {
            return new PersistentQueue<T>(storagePath, maxSize, throwOnConflict);
        }

        /// <inheritdoc/>
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)]
        public async Task<PersistentQueue> CreateAsync(string storagePath, CancellationToken cancellationToken = default)
        {
            var queue = new PersistentQueueImpl(storagePath, Constants._32Megabytes, true, true);
            await queue.InitializeAsync(cancellationToken);
            return new PersistentQueue(queue);
        }

        /// <inheritdoc/>
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)]
        public async Task<PersistentQueue> CreateAsync(string storagePath, int maxSize, bool throwOnConflict = true, CancellationToken cancellationToken = default)
        {
            var queue = new PersistentQueueImpl(storagePath, maxSize, throwOnConflict, true);
            await queue.InitializeAsync(cancellationToken);
            return new PersistentQueue(queue);
        }


        /// <inheritdoc/>
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)]
        public async Task<PersistentQueue<T>> CreateAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] T>(string storagePath, CancellationToken cancellationToken = default)
        {
            var queue = new PersistentQueueImpl<T>(storagePath, Constants._32Megabytes, true, true);
            await queue.InitializeAsync(cancellationToken);
            return new PersistentQueue<T>(queue);
        }

        /// <inheritdoc/>
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)]
        public async Task<PersistentQueue<T>> CreateAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] T>(string storagePath, int maxSize, bool throwOnConflict = true, CancellationToken cancellationToken = default)
        {
            var queue = new PersistentQueueImpl<T>(storagePath, Constants._32Megabytes, true, true);
            await queue.InitializeAsync(cancellationToken);
            return new PersistentQueue<T>(queue);
        }
    }

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
