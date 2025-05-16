namespace ModernDiskQueue.Implementation.Interfaces
{
    using ModernDiskQueue.Implementation;
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Interface for file access mechanism. For test and advanced users only.
    /// See the source code for more details.
    /// </summary>
    public interface IFileDriver
    {
        /// <summary>
        /// Proxy for Path.GetFullPath()
        /// </summary>
        string GetFullPath(string path);

        /// <summary>
        /// Proxy for Directory.Exists
        /// </summary>
        bool DirectoryExists(string path);

        /// <summary>
        /// Asynchronously returns true if a directory exists at the given path. False otherwise.
        /// </summary>
        ValueTask<bool> DirectoryExistsAsync(string path, CancellationToken cancellationToken = default);

        /// <summary>
        /// Proxy for Path.Combine
        /// </summary>
        string PathCombine(string a, string b);

        /// <summary>
        /// Try to get a lock on a file path
        /// </summary>
        Maybe<ILockFile> CreateLockFile(string path);

        /// <summary>
        /// Asynchronously tries to get a lock on a file path.
        /// </summary>
        Task<Maybe<ILockFile>> CreateLockFileAsync(string path, CancellationToken cancellationToken = default);

        /// <summary>
        /// Release a lock that was previously held
        /// </summary>
        void ReleaseLock(ILockFile fileLock);

        /// <summary>
        /// Release a lock that was previously held
        /// </summary>
        Task ReleaseLockAsync(ILockFile fileLock, CancellationToken cancellationToken = default);

        /// <summary>
        /// Ready a file for delete on next call to Finalise
        /// </summary>
        /// <param name="path"></param>
        void PrepareDelete(string path);

        /// <summary>
        /// Asynchronously prepares a file for deletion on the next call to FinaliseAsync.
        /// </summary>
        Task PrepareDeleteAsync(string path, CancellationToken cancellationToken = default);

        /// <summary>
        /// Complete any waiting file operations
        /// </summary>
        void Finalise();

        /// <summary>
        /// Asynchronously completes any waiting file operations.
        /// </summary>
        Task FinaliseAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Proxy for Directory.Create
        /// </summary>
        /// <param name="path"></param>
        void CreateDirectory(string path);

        /// <summary>
        /// Asynchronously creates a directory at the specified path.
        /// </summary>
        ValueTask CreateDirectoryAsync(string path, CancellationToken cancellationToken = default);

        /// <summary>
        /// Open a transaction log file as a stream
        /// </summary>
        IFileStream OpenTransactionLog(string path, int bufferLength);

        /// <summary>
        /// Asynchronously opens a transaction log file as a stream.
        /// </summary>
        Task<IFileStream> OpenTransactionLogAsync(string path, int bufferLength, CancellationToken cancellationToken = default);

        /// <summary>
        /// Open a data file for reading
        /// </summary>
        IFileStream OpenReadStream(string path);

        /// <summary>
        /// Asynchronously opens a data file for reading.
        /// </summary>
        Task<IFileStream> OpenReadStreamAsync(string path, CancellationToken cancellationToken = default);

        /// <summary>
        /// Open a data file for writing
        /// </summary>
        IFileStream OpenWriteStream(string dataFilePath);

        /// <summary>
        /// Asynchronously opens a data file for writing.
        /// </summary>
        Task<IFileStream> OpenWriteStreamAsync(string dataFilePath, CancellationToken cancellationToken = default);

        /// <summary>
        /// Run a read action over a file by name.
        /// Access is optimised for sequential scanning.
        /// No file share is permitted.
        /// </summary>
        bool AtomicRead(string path, Action<IBinaryReader> action);

        /// <summary>
        /// Asynchronously runs a read action over a file by name.
        /// Access is optimized for sequential scanning.
        /// No file share is permitted.
        /// </summary>
        Task<bool> AtomicReadAsync(string path, Func<IBinaryReader, Task> action, CancellationToken cancellationToken = default);

        /// <summary>
        /// Run a write action over a file by name.
        /// No file share is permitted.
        /// </summary>
        void AtomicWrite(string path, Action<IBinaryWriter> action);

        /// <summary>
        /// Asynchronously runs a write action over a file by name.
        /// No file share is permitted.
        /// </summary>
        Task AtomicWriteAsync(string path, Func<IBinaryWriter, Task> action, CancellationToken cancellationToken = default);

        /// <summary>
        /// Returns true if a readable file exists at the given path. False otherwise
        /// </summary>
        bool FileExists(string path);

        /// <summary>
        /// Asynchronously returns true if a readable file exists at the given path. False otherwise.
        /// </summary>
        ValueTask<bool> FileExistsAsync(string path, CancellationToken cancellationToken = default);

        /// <summary>
        /// Try to delete all files and directories in a sub-path
        /// </summary>
        void DeleteRecursive(string path);

        /// <summary>
        /// Try to delete all files and directories in a sub-path
        /// </summary>
        Task DeleteRecursiveAsync(string path, CancellationToken cancellationToken = default);
    }
}