using System.Runtime.CompilerServices;
[assembly: InternalsVisibleTo("ModernDiskQueue.Benchmarks")]
namespace ModernDiskQueue
{
    using ModernDiskQueue;
    using System;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// This class performs basic serialization from objects of Type T to byte arrays suitable for use in DiskQueue sessions.
    /// <p></p>
    /// Due to the constraints of supporting a wide range of target runtimes, this serializer is very primitive.
    /// You are free to implement your own <see cref="ISerializationStrategy{T}"/> and inject it into <see cref="PersistentQueue{T}"/>.
    /// </summary>
    /// <typeparam name="T">Type to be stored and retrieved. It must be either [Serializable] or a primitive type</typeparam>
    public class SerializationStrategyXml<T> : ISerializationStrategy<T>
    {
        private readonly DataContractSerializer _serialiser;

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializationStrategyXml{T}"/> class.
        /// </summary>
        /// <remarks>This constructor sets up the XML serialization strategy using a <see
        /// cref="DataContractSerializer"/> configured to preserve object references and serialize readonly types.</remarks>
        public SerializationStrategyXml()
        {
            var settings = new DataContractSerializerSettings
            {
                PreserveObjectReferences = true,
                SerializeReadOnlyTypes = true
            };
            _serialiser = new DataContractSerializer(typeof(T), settings);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializationStrategyXml{T}"/> class using the specified <see
        /// cref="DataContractSerializerSettings"/>.
        /// </summary>
        /// <param name="settings">The settings to configure the <see cref="DataContractSerializer"/> used for XML serialization.</param>
        public SerializationStrategyXml(DataContractSerializerSettings settings)
        {
            ArgumentNullException.ThrowIfNull(settings, nameof(settings));
            _serialiser = new DataContractSerializer(typeof(T), settings);
        }

        /// <inheritdoc />
        public T? Deserialize(byte[]? bytes)
        {
            if (bytes == null)
            {
                return default;
            }

            if (typeof(T) == typeof(string)) return (T)((object)Encoding.UTF8.GetString(bytes));

            using MemoryStream ms = new(bytes);
            var obj = _serialiser.ReadObject(ms);
            if (obj == null)
            {
                return default;
            }
            return (T)obj;
        }

        /// <summary>
        /// Asynchronously deserializes the specified byte array into an object of type <typeparamref name="T"/>.
        /// </summary>
        /// <remarks>If <paramref name="bytes"/> is <see langword="null"/> or empty, the method returns
        /// <see langword="null"/>. Ensure that the byte array contains valid serialized data compatible with the
        /// expected type <typeparamref name="T"/>.</remarks>
        /// <param name="bytes">The byte array containing the serialized data. Can be <see langword="null"/> or empty.</param>
        /// <param name="cancellationToken">A token to monitor for cancellation requests. The operation does not support partial cancellation.</param>
        /// <returns>A <see cref="ValueTask{T}"/> representing the asynchronous operation. The result is an object of type
        /// <typeparamref name="T"/> if deserialization is successful; otherwise, <see langword="null"/>.</returns>
        public ValueTask<T?> DeserializeAsync(byte[]? bytes, CancellationToken cancellationToken = default)
        {
            return new ValueTask<T?>(Deserialize(bytes));
        }



        /// <inheritdoc />
        public byte[]? Serialize(T? obj)
        {
            if (obj == null)
            {
                return null;
            }

            if (typeof(T) == typeof(string)) return Encoding.UTF8.GetBytes(obj.ToString() ?? string.Empty);

            using MemoryStream ms = new();
            _serialiser.WriteObject(ms, obj);
            return ms.ToArray();
        }

        /// <summary>
        /// Asynchronously serializes the specified object into a byte array.
        /// </summary>
        /// <param name="obj">The object to serialize. Can be <see langword="null"/>.</param>
        /// <param name="cancellationToken">A token to monitor for cancellation requests. Defaults to <see cref="CancellationToken.None"/>.</param>
        /// <returns>A task representing the asynchronous operation. The result contains the serialized byte array,  or <see
        /// langword="null"/> if the input object is <see langword="null"/>.</returns>
        public ValueTask<byte[]?> SerializeAsync(T? obj, CancellationToken cancellationToken = default)
        {
            return new ValueTask<byte[]?>(Serialize(obj));
        }
    }
}