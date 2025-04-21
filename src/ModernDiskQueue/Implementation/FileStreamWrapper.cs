using System.Runtime.CompilerServices;

[assembly:InternalsVisibleTo("ModernDiskQueue.Tests")]
namespace ModernDiskQueue.Implementation
{
    using ModernDiskQueue.PublicInterfaces;
    using System;
    using System.Buffers;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;

    internal class FileStreamWrapper : IFileStream, IBinaryReader, IBinaryWriter
    {
        private readonly Stream? _base;

        public FileStreamWrapper(Stream stream)
        {
            _base = stream;
        }

        public void Dispose()
        {
            if (_base is null) return;
            _base.Dispose();
        }

        public async ValueTask DisposeAsync()
        {
            if (_base is null) return;
            await _base.DisposeAsync().ConfigureAwait(false);
        }

        public long Write(byte[] bytes)
        {
            if (_base is null) throw new Exception("Tried to write to a disposed FileStream");
            _base.Write(bytes, 0, bytes.Length);
            return _base.Position;
        }

        /// <summary>
        /// This is an older legacy method that predates recent async refactoring efforts.
        /// It's overloaded by WriteAsync accepting a cancellation token.
        /// </summary>
        [Obsolete("Use WriteAsync(byte[] bytes, CancellationToken cancellationToken) instead.")]
        public async Task<long> WriteAsync(byte[] bytes)
        {
            if (_base is null) throw new Exception("Tried to write to a disposed FileStream");
            await _base.WriteAsync(bytes, 0, bytes.Length)!.ConfigureAwait(false);
            return _base.Position;
        }

        public async ValueTask<long> WriteAsync(byte[] bytes, CancellationToken cancellationToken)
        {
            if (_base is null) throw new Exception("Tried to write to a disposed FileStream");
            await _base.WriteAsync(bytes.AsMemory(0, bytes.Length), cancellationToken)!.ConfigureAwait(false);
            return _base.Position;
        }

        public async ValueTask<long> WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken cancellationToken)
        {
            if (_base is null) throw new Exception("Tried to write to a disposed FileStream");
            await _base.WriteAsync(bytes, cancellationToken)!.ConfigureAwait(false);

            return _base.Position;
        }

        public void Flush()
        {
            if (_base is null) throw new Exception("Tried to flush a disposed FileStream");
            if (_base is FileStream fs) fs.Flush(flushToDisk: true);
            else _base.Flush();
        }

        public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
        {
            if (_base is null) throw new Exception("Tried to flush a disposed FileStream");
            if (_base is FileStream fs)
            {
                // FileStream.FlushAsync doesn't support flushToDisk parameter, so we use Task.Run for the synchronous call.
                // Compared to the ms.Flush no-op method, FS flush is comparatively slow and can block, so that's why
                // wrapped in Task.Run.
                await Task.Run(() => fs.Flush(true), cancellationToken).ConfigureAwait(false);
            }
            else if (_base is MemoryStream ms)
            {
                // MemoryStream.FlushAsync will not throw an exception when trying to write beyond its capacity. It's a no-op method
                // per documentation, so I don't think there's any issue in calling FlushAsync and swallowing the exception
                // but for the sake of consistency with behavior of original sync operation, I'm calling the sync method to get that
                // exception thrown if appropriate.
                ms.Flush();
            }
            else
            {
                await _base.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        public void MoveTo(long offset) => _base?.Seek(offset, SeekOrigin.Begin);
        public int Read(byte[] buffer, int offset, int length) => _base?.Read(buffer, offset, length) ?? 0;

        public async ValueTask<int> ReadAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken = default)
        {
            if (_base is null) throw new Exception("Tried to read from a disposed FileStream");
            return await _base.ReadAsync(buffer.AsMemory(offset, length), cancellationToken).ConfigureAwait(false);
        }

        public IBinaryReader GetBinaryReader() => this;

        public void SetLength(long length) => _base?.SetLength(length);

        public Task SetLengthAsync(long length, CancellationToken cancellationToken = default)
        {
            if (_base is null) throw new Exception("Tried to set length on a disposed FileStream");
            return Task.Run(() => _base.SetLength(length), cancellationToken);
        }

        public void SetPosition(long position) => _base?.Seek(position, SeekOrigin.Begin);

        public void Truncate()
        {
            SetLength(0);
        }

        public async Task TruncateAsync(CancellationToken cancellationToken = default)
        {
            await SetLengthAsync(0, cancellationToken).ConfigureAwait(false);
        }

        public int ReadInt32()
        {
            if (_base is null) throw new Exception("Tried to read from a disposed FileStream");
            var d = _base.ReadByte();
            var c = _base.ReadByte();
            var b = _base.ReadByte();
            var a = _base.ReadByte();
            if (a < 0 || b < 0 || c < 0 || d < 0)
            {
                throw new EndOfStreamException(); // truncated
            }
            return a << 24 | b << 16 | c << 8 | d;
        }

        public async ValueTask<int> ReadInt32Async(CancellationToken cancellationToken = default)
        {
            if (_base is null) throw new Exception("Tried to read from a disposed FileStream");

            var buffer = ArrayPool<byte>.Shared.Rent(4);
            try
            {
                var bytesRead = await _base.ReadAsync(buffer.AsMemory(0, 4), cancellationToken).ConfigureAwait(false);
                if (bytesRead < 4) throw new EndOfStreamException();

                return buffer[3] << 24 | buffer[2] << 16 | buffer[1] << 8 | buffer[0];
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        public byte ReadByte()
        {
            if (_base is null) throw new Exception("Tried to read from a disposed FileStream");
            return (byte)_base.ReadByte();
        }

        public async ValueTask<byte> ReadByteAsync(CancellationToken cancellationToken = default)
        {
            if (_base is null) throw new Exception("Tried to read from a disposed FileStream");

            var buffer = ArrayPool<byte>.Shared.Rent(1);
            try
            {
                var bytesRead = await _base.ReadAsync(buffer.AsMemory(0, 1), cancellationToken).ConfigureAwait(false);
                if (bytesRead < 1) throw new EndOfStreamException();

                return buffer[0];
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        public byte[] ReadBytes(int count)
        {
            if (_base is null) throw new Exception("Tried to read from a disposed FileStream");
            var buffer = new byte[count];
            var actual = _base.Read(buffer, 0, count);
            if (actual != count) return Array.Empty<byte>();
            return buffer;
        }

        public async ValueTask<byte[]> ReadBytesAsync(int count, CancellationToken cancellationToken = default)
        {
            if (_base is null) throw new Exception("Tried to read from a disposed FileStream");

            var buffer = new byte[count];
            var bytesRead = await _base.ReadAsync(buffer.AsMemory(0, count), cancellationToken).ConfigureAwait(false);

            if (bytesRead != count) return Array.Empty<byte>();
            return buffer;
        }

        public long GetLength()
        {
            if (_base is null) throw new Exception("Tried to read from a disposed FileStream");
            return _base.Length;
        }

        public long GetPosition() => _base?.Position ?? 0;
        public long ReadInt64()
        {
            var b = (long)ReadInt32();
            var a = (long)ReadInt32();
            return a << 32 | b;
        }

        public async Task<long> ReadInt64Async(CancellationToken cancellationToken = default)
        {
            var b = (long)await ReadInt32Async(cancellationToken).ConfigureAwait(false);
            var a = (long)await ReadInt32Async(cancellationToken).ConfigureAwait(false);
            return a << 32 | b;
        }
    }
}