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

        public async Task<long> WriteAsync(byte[] bytes)
        {
            if (_base is null) throw new Exception("Tried to write to a disposed FileStream");
            await _base.WriteAsync(bytes, 0, bytes.Length)!.ConfigureAwait(false);
            return _base.Position;
        }

        public async Task<long> WriteAsync(byte[] bytes, CancellationToken cancellationToken)
        {
            if (_base is null) throw new Exception("Tried to write to a disposed FileStream");
            await _base.WriteAsync(bytes, 0, bytes.Length, cancellationToken)!.ConfigureAwait(false);
            return _base.Position;
        }

        public void Flush()
        {
            if (_base is null) throw new Exception("Tried to flush a disposed FileStream");
            if (_base is FileStream fs) fs.Flush(flushToDisk: true);
            else _base.Flush();
        }

        public async Task FlushAsync(CancellationToken cancellationToken = default)
        {
            if (_base is null) throw new Exception("Tried to flush a disposed FileStream");
            if (_base is FileStream fs)
            {
                // FileStream.FlushAsync doesn't support flushToDisk parameter, so we use Task.Run for the synchronous call
                await Task.Run(() => fs.Flush(true), cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await _base.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        public void MoveTo(long offset) => _base?.Seek(offset, SeekOrigin.Begin);
        public int Read(byte[] buffer, int offset, int length) => _base?.Read(buffer, offset, length) ?? 0;

        public async Task<int> ReadAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken = default)
        {
            if (_base is null) throw new Exception("Tried to read from a disposed FileStream");
            return await _base.ReadAsync(buffer, offset, length, cancellationToken).ConfigureAwait(false);
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
            if (a < 0 || b < 0 || c < 0 || d < 0) throw new EndOfStreamException(); // truncated
            
            return a << 24 | b << 16 | c << 8 | d;
        }

        public async Task<int> ReadInt32Async(CancellationToken cancellationToken = default)
        {
            if (_base is null) throw new Exception("Tried to read from a disposed FileStream");

            var buffer = ArrayPool<byte>.Shared.Rent(4);
            try
            {
                var bytesRead = await _base.ReadAsync(buffer, 0, 4, cancellationToken).ConfigureAwait(false);
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

        public async Task<byte> ReadByteAsync(CancellationToken cancellationToken = default)
        {
            if (_base is null) throw new Exception("Tried to read from a disposed FileStream");

            var buffer = ArrayPool<byte>.Shared.Rent(1);
            try
            {
                var bytesRead = await _base.ReadAsync(buffer, 0, 1, cancellationToken).ConfigureAwait(false);
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

        public async Task<byte[]> ReadBytesAsync(int count, CancellationToken cancellationToken = default)
        {
            if (_base is null) throw new Exception("Tried to read from a disposed FileStream");

            var buffer = new byte[count];
            var bytesRead = await _base.ReadAsync(buffer, 0, count, cancellationToken).ConfigureAwait(false);

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