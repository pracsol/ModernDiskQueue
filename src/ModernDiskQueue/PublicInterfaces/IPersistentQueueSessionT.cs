using ModernDiskQueue.Implementation;
using System.Threading.Tasks;
using System.Threading;

namespace ModernDiskQueue.PublicInterfaces
{
    /// <inheritdoc cref="IPersistentQueueSession{T}"/>
    public interface IPersistentQueueSession<T> : IPersistentQueueSession
    {
        /// <summary>
        /// Queue data for a later decode. Data is written on `Flush()`
        /// </summary>
        void Enqueue(T data);

        /// <summary>
        /// Asynchronously queue data for a later decode. Data is written on `FlushAsync()`
        /// </summary>
        ValueTask EnqueueAsync(T data, CancellationToken cancellationToken = default);

        /// <summary>
        /// Try to pull data from the queue. Data is removed from the queue on `Flush()`
        /// </summary>
        new T? Dequeue();

        /// <summary>
        /// Asynchronously try to pull data from the queue. Data is removed from the queue on `FlushAsync()`
        /// </summary>
        new ValueTask<T?> DequeueAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// This class performs the serialization of the object to be queued into a byte array suitable for queueing.
        /// It defaults to <see cref="DefaultSerializationStrategy{T}"/>, but you are free to implement your own and inject it in.
        /// </summary>
        ISerializationStrategy<T> SerializationStrategy { get; set; }
    }
}
