using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ModernDiskQueue.Implementation;
using ModernDiskQueue.PublicInterfaces;

namespace ModernDiskQueue.Tests
{
    public class WriteFailureDriver : IFileDriver
    {
        private readonly StandardFileDriver _realDriver;

        public WriteFailureDriver()
        {
            _realDriver = new StandardFileDriver();
        }
        
        public string GetFullPath(string path)=> Path.GetFullPath(path);
        public bool DirectoryExists(string path) => Directory.Exists(path);
        public string PathCombine(string a, string b) => Path.Combine(a,b);

        public Maybe<ILockFile> CreateLockFile(string path)
        {
            throw new IOException("Sample CreateLockFile error");
        }

        public Task<Maybe<ILockFile>> CreateLockFileAsync(string path, CancellationToken cancellationToken = default)
        {
            throw new IOException("Sample CreateLockFile error");
        }

        public void ReleaseLock(ILockFile fileLock) { }

        public Task ReleaseLockAsync(ILockFile fileLock, CancellationToken cancellationToken = default) 
        {
            throw new NotImplementedException();
        }

        public void PrepareDelete(string path)
        {
            _realDriver.PrepareDelete(path);
        }

        public Task PrepareDeleteAsync(string path, CancellationToken cancellationToken = default)
        {
            return ((IFileDriver)_realDriver).PrepareDeleteAsync(path, cancellationToken);
        }

        public void Finalise()
        {
            _realDriver.Finalise();
        }

        public Task FinaliseAsync(CancellationToken cancellationToken = default)
        {
            return ((IFileDriver)_realDriver).FinaliseAsync(cancellationToken);
        }

        public void CreateDirectory(string path) { }

        public Task CreateDirectoryAsync(string path, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }
        public IFileStream OpenTransactionLog(string path, int bufferLength)
        {
            return _realDriver.OpenTransactionLog(path, bufferLength);
        }

        public Task<IFileStream> OpenTransactionLogAsync(string path, int bufferLength, CancellationToken cancellationToken = default)
        {
            return ((IFileDriver)_realDriver).OpenTransactionLogAsync(path, bufferLength, cancellationToken);
        }

        public IFileStream OpenReadStream(string path)
        {
            return _realDriver.OpenReadStream(path);
        }

        public Task<IFileStream> OpenReadStreamAsync(string path, CancellationToken cancellationToken = default)
        {
            return ((IFileDriver)_realDriver).OpenReadStreamAsync(path, cancellationToken);
        }
        public IFileStream OpenWriteStream(string dataFilePath)
        {
            throw new IOException("Sample OpenWriteStream error");
        }

        public Task<IFileStream> OpenWriteStreamAsync(string dataFilePath, CancellationToken cancellationToken = default)
        {
            throw new IOException("Sample OpenWriteStream error");
        }

        public bool AtomicRead(string path, Action<IBinaryReader> action)
        {
            return _realDriver.AtomicRead(path, action);
        }

        public Task<bool> AtomicReadAsync(string path, Func<IBinaryReader, Task> action, CancellationToken cancellationToken = default)
        {
            return ((IFileDriver)_realDriver).AtomicReadAsync(path, action, cancellationToken);
        }

        public void AtomicWrite(string path, Action<IBinaryWriter> action)
        {
            throw new IOException("Sample AtomicWrite error");
        }

        public Task AtomicWriteAsync(string path, Func<IBinaryWriter, Task> action, CancellationToken cancellationToken = default)
        {
            throw new IOException("Sample AtomicWrite error");
        }

        public bool FileExists(string path)
        {
            return _realDriver.FileExists(path);
        }

        public Task<bool> FileExistsAsync(string path, CancellationToken cancellationToken = default)
        {
            return ((IFileDriver)_realDriver).FileExistsAsync(path, cancellationToken);
        }
        public void DeleteRecursive(string path) { }

        public Task DeleteRecursiveAsync(string path, CancellationToken cancellationToken = default) 
        {
            throw new NotImplementedException();
        }

        public Task<bool> DirectoryExistsAsync(string path, CancellationToken cancellationToken = default)
        {
            return ((IFileDriver)_realDriver).DirectoryExistsAsync(path, cancellationToken);
        }
    }
}