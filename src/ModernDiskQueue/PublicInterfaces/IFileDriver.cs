using ModernDiskQueue.Implementation;
using System;

namespace ModernDiskQueue.PublicInterfaces
{
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
        /// Proxy for Path.Combine
        /// </summary>
        string PathCombine(string a, string b);

        /// <summary>
        /// Try to get a lock on a file path
        /// </summary>
        Maybe<ILockFile> CreateLockFile(string path);

        /// <summary>
        /// Release a lock that was previously held
        /// </summary>
        void ReleaseLock(ILockFile fileLock);

        /// <summary>
        /// Ready a file for delete on next call to Finalise
        /// </summary>
        /// <param name="path"></param>
        void PrepareDelete(string path);

        /// <summary>
        /// Complete any waiting file operations
        /// </summary>
        void Finalise();

        /// <summary>
        /// Proxy for Directory.Create
        /// </summary>
        /// <param name="path"></param>
        void CreateDirectory(string path);

        /// <summary>
        /// Open a transaction log file as a stream
        /// </summary>
        IFileStream OpenTransactionLog(string path, int bufferLength);

        /// <summary>
        /// Open a data file for reading
        /// </summary>
        IFileStream OpenReadStream(string path);

        /// <summary>
        /// Open a data file for writing
        /// </summary>
        IFileStream OpenWriteStream(string dataFilePath);

        /// <summary>
        /// Run a read action over a file by name.
        /// Access is optimised for sequential scanning.
        /// No file share is permitted.
        /// </summary>
        bool AtomicRead(string path, Action<IBinaryReader> action);

        /// <summary>
        /// Run a write action over a file by name.
        /// No file share is permitted.
        /// </summary>
        void AtomicWrite(string path, Action<IBinaryWriter> action);

        /// <summary>
        /// Returns true if a readable file exists at the given path. False otherwise
        /// </summary>
        bool FileExists(string path);

        /// <summary>
        /// Try to delete all files and directories in a sub-path
        /// </summary>
        void DeleteRecursive(string path);
    }
}