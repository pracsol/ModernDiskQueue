using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ModernDiskQueue
{
    /// <summary>
    /// Factory for creating <see cref="PersistentQueue{T}"/> instances.
    /// </summary>
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors |
                               DynamicallyAccessedMemberTypes.PublicMethods)]
    public class PersistentQueueFactory : IPersistentQueueFactory
    {
        /// <inheritdoc/>
        public PersistentQueue<T> Create<T>(string storagePath)
        {
            return new PersistentQueue<T>(storagePath);
        }

        /// <inheritdoc/>
        public PersistentQueue<T> Create<T>(string storagePath, int maxSize, bool throwOnConflict = true)
        {
            return new PersistentQueue<T>(storagePath, maxSize, throwOnConflict);
        }

        /// <inheritdoc/>
        [RequiresUnreferencedCode("This method creates a generic instance and may be removed during trimming")]
        public async Task<PersistentQueue<T>> CreateAsync<T>(string storagePath, CancellationToken cancellationToken = default)
        {
            var baseQueue = await PersistentQueue.CreateAsync(storagePath, cancellationToken);
            var impl = baseQueue.Internals;
            baseQueue.Internals = null; // Prevent double disposal
            await baseQueue.DisposeAsync();

            return new PersistentQueue<T> { Queue = impl };
        }

        /// <inheritdoc/>
        [RequiresUnreferencedCode("This method creates a generic instance and may be removed during trimming")]
        public async Task<PersistentQueue<T>> CreateAsync<T>(string storagePath, int maxSize, bool throwOnConflict = true, CancellationToken cancellationToken = default)
        {
            var baseQueue = await PersistentQueue.CreateAsync(storagePath, maxSize, throwOnConflict, cancellationToken);
            var impl = baseQueue.Internals;
            baseQueue.Internals = null; // Prevent double disposal
            await baseQueue.DisposeAsync();

            return new PersistentQueue<T> { Queue = impl };
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
