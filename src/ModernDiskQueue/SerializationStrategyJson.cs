
namespace ModernDiskQueue.Implementation
{
    using ModernDiskQueue;
    using System;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Text.Json;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// This class performs basic serialization from objects of Type T to byte arrays suitable for use in DiskQueue sessions.
    /// <p></p>
    /// Due to the constraints of supporting a wide range of target runtimes, this serializer is very primitive.
    /// You are free to implement your own <see cref="ISerializationStrategy{T}"/> and inject it into <see cref="PersistentQueue{T}"/>.
    /// </summary>
    /// <typeparam name="T">Type to be stored and retrieved. It must be either [Serializable] or a primitive type</typeparam>
    public class SerializationStrategyJson<T> : ISerializationStrategy<T>
    {
        private readonly JsonSerializerOptions defaultOptions = JsonSerializerOptions.Default;

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializationStrategyJson{T}"/> class with default serialization options. 
        /// </summary>
        public SerializationStrategyJson()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializationStrategyJson{T}"/> class using the specified JSON
        /// serializer options.
        /// </summary>
        /// <param name="options">The <see cref="JsonSerializerOptions"/> to configure the JSON serialization and deserialization behavior.
        /// Must not be <see langword="null"/>.</param>
        public SerializationStrategyJson(JsonSerializerOptions options)
        {
            ArgumentNullException.ThrowIfNull(options, nameof(options));
            defaultOptions = options;
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
            var obj = JsonSerializer.Deserialize<T>(ms);
            if (obj == null)
            {
                return default;
            }
            return (T)obj;
        }

        /// <summary>
        /// Asynchronously deserializes the specified byte array into an object of type <typeparamref name="T"/>.
        /// </summary>
        /// <remarks>If <paramref name="bytes"/> is <see langword="null"/>, the method returns <see
        /// langword="null"/>. Ensure that the byte array contains valid serialized data compatible with the expected
        /// type <typeparamref name="T"/>.</remarks>
        /// <param name="bytes">The byte array containing the serialized data. Can be <see langword="null"/>.</param>
        /// <param name="cancellationToken">A token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns>A <see cref="ValueTask{T}"/> representing the asynchronous operation. The result is an object of type
        /// <typeparamref name="T"/> if deserialization is successful; otherwise, <see langword="null"/>.</returns>
        public async ValueTask<T?> DeserializeAsync(byte[]? bytes, CancellationToken cancellationToken = default)
        {
            return await DeserializeAsync(bytes, defaultOptions, cancellationToken);
        }

        /// <summary>
        /// Deserializes the specified byte array into an object of type <typeparamref name="T"/> asynchronously.
        /// </summary>
        /// <remarks>If the type <typeparamref name="T"/> is <see cref="string"/>, the method interprets
        /// the byte array as UTF-8 encoded text and returns the corresponding string.</remarks>
        /// <param name="bytes">The byte array containing the serialized data. If <see langword="null"/>, the method returns <c>default</c>.</param>
        /// <param name="options">The <see cref="JsonSerializerOptions"/> to configure the deserialization process. This parameter is
        /// optional.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> to observe while waiting for the operation to complete. This parameter is
        /// optional.</param>
        /// <returns>A <see cref="ValueTask{T}"/> representing the asynchronous operation. The result is an object of type
        /// <typeparamref name="T"/> deserialized from the byte array, or <c>default</c> if the byte array is
        /// <see langword="null"/> or the deserialization fails.</returns>
        public async ValueTask<T?> DeserializeAsync(byte[]? bytes, JsonSerializerOptions options, CancellationToken cancellationToken = default)
        {
            if (bytes == null)
            {
                return default;
            }

            if (typeof(T) == typeof(string)) return (T)((object)Encoding.UTF8.GetString(bytes));

            using MemoryStream ms = new(bytes);
            var obj = await JsonSerializer.DeserializeAsync<T>(ms, options, cancellationToken);
            if (obj == null)
            {
                return default;
            }
            return (T)obj;
        }

        /// <inheritdoc />
        public byte[]? Serialize(T? obj)
        {
            return Serialize(obj, defaultOptions);
        }

        /// <inheritdoc />
        public byte[]? Serialize(T? obj, JsonSerializerOptions options)
        {
            if (obj == null)
            {
                return null;
            }

            if (typeof(T) == typeof(string)) return Encoding.UTF8.GetBytes(obj.ToString() ?? string.Empty);

            using MemoryStream ms = new();
            JsonSerializer.Serialize<T>(ms, obj, options);//.WriteObject(ms, obj);
            return ms.ToArray();
        }

        /// <summary>
        /// Asynchronously serializes the specified object into a byte array.
        /// </summary>
        /// <param name="obj">The object to serialize. Can be <see langword="null"/>.</param>
        /// <param name="cancellationToken">A token to monitor for cancellation requests. Defaults to <see cref="CancellationToken.None"/>.</param>
        /// <returns>A <see cref="ValueTask{TResult}"/> representing the asynchronous operation.  The result is a byte array
        /// containing the serialized representation of the object, or <see langword="null"/> if the object is <see
        /// langword="null"/>.</returns>
        public async ValueTask<byte[]?> SerializeAsync(T? obj, CancellationToken cancellationToken = default)
        {
            return await SerializeAsync(obj, defaultOptions, cancellationToken);
        }

        /// <summary>
        /// Asynchronously serializes the specified object to a JSON byte array.
        /// </summary>
        /// <remarks>If the type of <typeparamref name="T"/> is <see cref="string"/>, the method directly
        /// converts the string to a UTF-8 encoded byte array. For other types, the object is serialized using <see
        /// cref="JsonSerializer"/> and written to a memory stream.</remarks>
        /// <param name="obj">The object to serialize. If <paramref name="obj"/> is <see langword="null"/>, the method returns <see
        /// langword="null"/>.</param>
        /// <param name="options">The <see cref="JsonSerializerOptions"/> to configure the serialization process.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> to observe while waiting for the operation to complete.</param>
        /// <returns>A <see cref="ValueTask{TResult}"/> representing the asynchronous operation. The result is a byte array
        /// containing the serialized JSON representation of the object,  or <see langword="null"/> if <paramref
        /// name="obj"/> is <see langword="null"/>.</returns>
        public async ValueTask<byte[]?> SerializeAsync(T? obj, JsonSerializerOptions options, CancellationToken cancellationToken = default)
        {
            if (obj == null)
            {
                return null;
            }

            if (typeof(T) == typeof(string))
            {
                return Encoding.UTF8.GetBytes(obj.ToString() ?? string.Empty);
            }
            using MemoryStream ms = new();
            await JsonSerializer.SerializeAsync(ms, obj, options, cancellationToken);
            return ms.ToArray();
        }
    }
}