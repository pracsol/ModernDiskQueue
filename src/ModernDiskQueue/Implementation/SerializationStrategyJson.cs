
namespace ModernDiskQueue.Implementation
{
    using ModernDiskQueue;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Text.Json;

    /// <summary>
    /// This class performs basic serialization from objects of Type T to byte arrays suitable for use in DiskQueue sessions.
    /// <p></p>
    /// Due to the constraints of supporting a wide range of target runtimes, this serializer is very primitive.
    /// You are free to implement your own <see cref="ISerializationStrategy{T}"/> and inject it into <see cref="PersistentQueue{T}"/>.
    /// </summary>
    /// <typeparam name="T">Type to be stored and retrieved. It must be either [Serializable] or a primitive type</typeparam>
    internal class SerializationStrategyJson<T> : ISerializationStrategy<T>
    {
        private readonly JsonSerializerOptions defaultOptions = JsonSerializerOptions.Default;

        public SerializationStrategyJson()
        {
        }

        public SerializationStrategyJson(JsonSerializerOptions options)
        {
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

        public async ValueTask<T?> DeserializeAsync(byte[]? bytes, CancellationToken cancellationToken = default)
        {
            return await DeserializeAsync(bytes, defaultOptions, cancellationToken);
        }

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

        public async ValueTask<byte[]?> SerializeAsync(T? obj, CancellationToken cancellationToken = default)
        {
            return await SerializeAsync(obj, defaultOptions, cancellationToken);
        }

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