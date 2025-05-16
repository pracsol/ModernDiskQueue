namespace ModernDiskQueue.Implementation
{
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.Abstractions;
    using Microsoft.Extensions.Options;
    using ModernDiskQueue.Implementation.Interfaces;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Threading;
    using System.Threading.Channels;
    using System.Threading.Tasks;

    /// <summary>
    /// A wrapper around System.IO.File to help with
    /// heavily multi-threaded and multi-process workflows
    /// </summary>
    internal class StandardFileDriver : IFileDriver
    {
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger<StandardFileDriver> _logger;
        private readonly ModernDiskQueueOptions _options;
        public const int RetryLimit = 15;

        // existing fields for sync operations
        private static readonly object _lock = new();
        private static readonly Queue<string> _waitingDeletes = new();

        // New channel for async operations
        private readonly Channel<string> _waitingDeletesAsync = Channel.CreateUnbounded<string>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true,
            AllowSynchronousContinuations = false,
        });

        public readonly AsyncLock _asyncLock = new();
        private readonly AsyncLocal<bool> _holdsLock = new();

        public StandardFileDriver()
        {
            _loggerFactory = NullLoggerFactory.Instance;
            _logger = NullLogger<StandardFileDriver>.Instance;
            _options = new();
        }

        public StandardFileDriver(ILoggerFactory loggerFactory, IOptions<ModernDiskQueueOptions> options)
        {
            _loggerFactory = loggerFactory;
            _logger = loggerFactory?.CreateLogger<StandardFileDriver>() ?? NullLogger<StandardFileDriver>.Instance;
            _options = options.Value;
        }

        public string GetFullPath(string path) => Path.GetFullPath(path);
        public string PathCombine(string a, string b) => Path.Combine(a, b);

        /// <summary>
        /// Test for the existence of a directory
        /// </summary>
        public bool DirectoryExists(string path)
        {
            lock (_lock)
            {
                return Directory.Exists(path);
            }
        }

        /// <summary>
        /// Asynchronously test for the existence of a directory
        /// </summary>
        public async ValueTask<bool> DirectoryExistsAsync(string path, CancellationToken cancellationToken = default)
        {
            if (_holdsLock.Value)
            {
                return DirectoryExists_UnderLock(path);
            }
            else
            {
                try
                {
                    using (await _asyncLock.LockAsync("SFD", cancellationToken).ConfigureAwait(false))
                    {
                        _holdsLock.Value = true;
                        return DirectoryExists_UnderLock(path);
                    }
                }
                finally
                {
                    _holdsLock.Value = false;
                }
            }
        }

        /// <summary>
        /// Test for the existence of a directory
        /// <para>WARNING: Caller must have lock on <see cref="_asyncLock"/></para>
        /// </summary>
        private static bool DirectoryExists_UnderLock(string path)
        {
            return Directory.Exists(path);
        }

        /// <summary>
        /// Moves a file to a temporary name and adds it to an internal
        /// delete list. Files are permanently deleted on a call to Finalise()
        /// </summary>
        public void PrepareDelete(string path)
        {
            lock (_lock)
            {
                if (!FileExists(path)) return;
                var dir = Path.GetDirectoryName(path) ?? "";
                var file = Path.GetFileNameWithoutExtension(path);
                var prefix = Path.GetRandomFileName();

                var deletePath = Path.Combine(dir, $"{file}_dc_{prefix}");

                if (Move(path, deletePath))
                {
                    _waitingDeletes.Enqueue(deletePath);
                }
            }
        }

        /// <summary>
        /// Asynchronously moves a file to a temporary name and adds it to an internal
        /// delete list. Files are permanently deleted on a call to FinaliseAsync()
        /// </summary>
        public async Task PrepareDeleteAsync(string path, CancellationToken cancellationToken = default)
        {

            bool fileExists = await FileExistsAsync(path, cancellationToken).ConfigureAwait(false);
            if (!fileExists) return;

            var dir = Path.GetDirectoryName(path) ?? "";
            var file = Path.GetFileNameWithoutExtension(path);
            var prefix = Path.GetRandomFileName();
            var deletePath = Path.Combine(dir, $"{file}_dc_{prefix}");
            bool isFileMoved = false;

            try
            {
                using (await _asyncLock.LockAsync("SFD", cancellationToken).ConfigureAwait(false))
                {
                    _holdsLock.Value = true;
                    isFileMoved = await MoveAsync(path, deletePath, cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                _holdsLock.Value = false;
            }

            if (isFileMoved)
            {
                await _waitingDeletesAsync.Writer.WriteAsync(deletePath, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Commit any pending prepared operations.
        /// Each operation will happen in serial.
        /// </summary>
        public void Finalise()
        {
            lock (_lock)
            {
                while (_waitingDeletes.Count > 0)
                {
                    var path = _waitingDeletes.Dequeue();
                    if (path is null) continue;
                    File.Delete(path);
                }
            }
        }

        /// <summary>
        /// Asynchronously commit any pending prepared operations.
        /// Each operation will happen in serial.
        /// </summary>
        public async Task FinaliseAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                using (await _asyncLock.LockAsync("SFD", cancellationToken).ConfigureAwait(false))
                {
                    _holdsLock.Value = true;
                    while (_waitingDeletesAsync.Reader.TryRead(out string? path))
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        if (path is null)
                        {
                            Console.WriteLine("FinaliseAsync is waiting for file. zzzz... :(");
                            continue;
                        }
                        File.Delete(path);
                    }
                    return;
                }
            }
            finally
            {
                _holdsLock.Value = false;
            }
        }

        /// <summary>
        /// Create and open a new file with no sharing between processes.
        /// </summary>
#pragma warning disable IDE0079 // Suppress warning about suppressing warnings
#pragma warning disable CA1859 // Use concrete types when possible for improved performance
        private static ILockFile CreateNoShareFile(string path)
#pragma warning restore CA1859, IDE0079
        {
            lock (_lock)
            {
                var currentProcess = Process.GetCurrentProcess();
                var currentLockData = new LockFileData
                {
                    ProcessId = currentProcess.Id,
                    ThreadId = Environment.CurrentManagedThreadId,
                    ProcessStart = GetProcessStartAsUnixTimeMs(currentProcess),
                };
                var keyBytes = MarshallHelper.Serialize(currentLockData);

                if (!File.Exists(path)) File.WriteAllBytes(path, keyBytes);
                var lockBytes = File.ReadAllBytes(path); // will throw if OS considers the file locked
                var fileLockData = MarshallHelper.Deserialize<LockFileData>(lockBytes);

                if (fileLockData.ThreadId != currentLockData.ThreadId || fileLockData.ProcessId != currentLockData.ProcessId)
                {
                    // The first two *should not* happen, but filesystems seem to have weird bugs.
                    // Is this for our own process?
                    if (fileLockData.ProcessId == currentLockData.ProcessId)
                    {
                        throw new Exception($"This queue is locked by another thread in this process. Thread id = {fileLockData.ThreadId}");
                    }

                    // Is it for a running process?
                    if (IsRunning(fileLockData))
                    {
                        throw new Exception($"This queue is locked by another running process. Process id = {fileLockData.ProcessId}");
                    }

                    // We have a lock from a dead process. Kill it.
                    File.Delete(path);
                    File.WriteAllBytes(path, keyBytes);
                }

                var lockStream = new FileStream(path,
                    FileMode.Create,
                    FileAccess.ReadWrite,
                    FileShare.None);

                lockStream.Write(keyBytes, 0, keyBytes.Length);
                lockStream.Flush(true);

                return new LockFile(lockStream, path);
            }
        }

        /// <summary>
        /// Asynchronously create and open a new file with no sharing between processes.
        /// </summary>
        private async Task<ILockFile> CreateNoShareFileAsync(string path, CancellationToken cancellationToken = default)
        {
            if (_holdsLock.Value)
            {
                return await CreateNoShareFile_UnderLock(path, cancellationToken);
            }
            else
            {
                try
                {
                    using (await _asyncLock.LockAsync("SFD", cancellationToken).ConfigureAwait(false))
                    {
                        _holdsLock.Value = true;
                        return await CreateNoShareFile_UnderLock(path, cancellationToken);
                    }
                }
                finally
                {
                    _holdsLock.Value = false;
                }
            }
        }

        /// <summary>
        /// Create and open a new file with no sharing between processes.
        /// <para>WARNING: Caller must have lock on <see cref="_asyncLock"/></para>
        /// </summary>
        private async Task<ILockFile> CreateNoShareFile_UnderLock(string path, CancellationToken cancellationToken = default)
        {
            _logger.LogTrace("Thread {ThreadID} attempting to create lock file.", Environment.CurrentManagedThreadId);
            try
            {
                var currentProcess = Process.GetCurrentProcess();
                var currentLockData = new LockFileData
                {
                    ProcessId = currentProcess.Id,
                    ThreadId = Environment.CurrentManagedThreadId,
                    ProcessStart = GetProcessStartAsUnixTimeMs(currentProcess),
                };
                var keyBytes = MarshallHelper.Serialize(currentLockData);

                if (!(await Task.Run(() => File.Exists(path), cancellationToken)))
                {
                    await File.WriteAllBytesAsync(path, keyBytes, cancellationToken);
                }

                var lockBytes = await File.ReadAllBytesAsync(path, cancellationToken); // will throw if OS considers the file locked
                var fileLockData = MarshallHelper.Deserialize<LockFileData>(lockBytes);

                if (fileLockData.ThreadId != currentLockData.ThreadId || fileLockData.ProcessId != currentLockData.ProcessId)
                {
                    // The first two *should not* happen, but filesystems seem to have weird bugs.
                    // Is this for our own process?
                    if (fileLockData.ProcessId == currentLockData.ProcessId)
                    {
                        throw new Exception($"This queue is locked by another thread in this process. Thread id = {fileLockData.ThreadId}");
                    }

                    // Is it for a running process?
                    if (IsRunning(fileLockData))
                    {
                        throw new Exception($"This queue is locked by another running process. Process id = {fileLockData.ProcessId}");
                    }

                    // We have a lock from a dead process. Kill it.
                    _logger.LogTrace("Overwriting the existing (but abandoned) lock file.");
                    await Task.Run(() => File.Delete(path), cancellationToken);
                    await File.WriteAllBytesAsync(path, keyBytes, cancellationToken);
                }

                var lockStream = new FileStream(path,
                        FileMode.Create,
                        FileAccess.ReadWrite,
                        FileShare.None);

                await lockStream.WriteAsync(keyBytes, cancellationToken);
                await lockStream.FlushAsync(cancellationToken);
                lockStream.Flush(true);
                _logger.LogTrace("Thread {ThreadID} created lock file at {path}", Environment.CurrentManagedThreadId, path);
                return new LockFile(lockStream, path);
            }
            catch (Exception ex)
            {
                _logger.LogTrace("Thread {ThreadID} failed to create lock file, {ErrorMessage}", Environment.CurrentManagedThreadId, ex.Message);
                throw;
            }
        }

        /// <summary>
        /// Return true if the processId matches a running process
        /// </summary>
        private static bool IsRunning(LockFileData lockData)
        {
            try
            {
                var p = Process.GetProcessById(lockData.ProcessId);
                var startTimeOffset = GetProcessStartAsUnixTimeMs(p);
                return startTimeOffset == lockData.ProcessStart;
            }
            catch (InvalidOperationException)
            {
                return false;
            }
            catch (ArgumentException)
            {
                return false;
            }
            catch
            {
                return true;
            }
        }

        private static long GetProcessStartAsUnixTimeMs(Process process)
            => ((DateTimeOffset)process.StartTime).ToUnixTimeMilliseconds();

        /// <summary>
        /// Test for the existence of a file
        /// </summary>
        public bool FileExists(string path)
        {
            lock (_lock)
            {
                return File.Exists(path);
            }
        }

        /// <summary>
        /// Asynchronously test for the existence of a file
        /// </summary>
        public async ValueTask<bool> FileExistsAsync(string path, CancellationToken cancellationToken = default)
        {
            return await Task.Run(() => File.Exists(path), cancellationToken);
        }

        /// <summary>
        /// Recursively deletes a directory and all its contents
        /// </summary>
        /// <param name="path">Path of the folder to delete.</param>
        public void DeleteRecursive(string path)
        {
            lock (_lock)
            {
                if (Path.GetPathRoot(path) == Path.GetFullPath(path)) throw new Exception("Request to delete root directory rejected");
                if (string.IsNullOrWhiteSpace(Path.GetDirectoryName(path)!)) throw new Exception("Request to delete root directory rejected");
                if (File.Exists(path)) throw new Exception("Tried to recursively delete a single file.");

                Directory.Delete(path, true);
            }
        }

        public async Task DeleteRecursiveAsync(string path, CancellationToken cancellationToken = default)
        {
            if (_holdsLock.Value)
            {
                DeleteRecursive_UnderLock(path);
            }
            else
            {
                try
                {
                    using (await _asyncLock.LockAsync("SFD", cancellationToken).ConfigureAwait(false))
                    {
                        _holdsLock.Value = true;
                        DeleteRecursive_UnderLock(path);
                    }
                }
                finally
                {
                    _holdsLock.Value = false;
                }
            }
        }

        public static void DeleteRecursive_UnderLock(string path)
        {
            if (Path.GetPathRoot(path) == Path.GetFullPath(path)) throw new Exception("Request to delete root directory rejected");
            if (string.IsNullOrWhiteSpace(Path.GetDirectoryName(path)!)) throw new Exception("Request to delete root directory rejected");
            if (File.Exists(path)) throw new Exception("Tried to recursively delete a single file.");

            Directory.Delete(path, true);
        }

        /// <summary>
        /// Try to get a lock on a file path.
        /// </summary>
        /// <param name="path">Path of the file to be locked.</param>
        /// <returns><see cref="ILockFile"/>.</returns>
        public Maybe<ILockFile> CreateLockFile(string path)
        {
            try
            {
                return CreateNoShareFile(path).Success();
            }
            catch (Exception ex)
            {
                return Maybe<ILockFile>.Fail(ex);
            }
        }

        /// <summary>
        /// Asynchronously try to get a lock on a file path
        /// </summary>
        /// <param name="path">Path of the file to be locked.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> if different from default.</param>
        /// <returns><see cref="ILockFile"/>.</returns>
        public async Task<Maybe<ILockFile>> CreateLockFileAsync(string path, CancellationToken cancellationToken = default)
        {
            try
            {
                var lockFile = await CreateNoShareFileAsync(path, cancellationToken).ConfigureAwait(false);
                return lockFile.Success();
            }
            catch (Exception ex)
            {
                return Maybe<ILockFile>.Fail(ex);
            }
        }

        public void ReleaseLock(ILockFile fileLock)
        {
            lock (_lock)
            {
                fileLock.Dispose();
            }
        }

        public async Task ReleaseLockAsync(ILockFile fileLock, CancellationToken cancellationToken = default)
        {
            if (_holdsLock.Value)
            {
                await fileLock.DisposeAsync().ConfigureAwait(false);
            }
            else
            {
                try
                {
                    using (await _asyncLock.LockAsync("SFD", cancellationToken).ConfigureAwait(false))
                    {
                        await fileLock.DisposeAsync().ConfigureAwait(false);
                    }
                }
                finally
                {
                    _holdsLock.Value = false;
                }
            }
            _logger.LogTrace("Thread {ThreadID} released lock file.", Environment.CurrentManagedThreadId);
        }

        /// <summary>
        /// Attempt to create a directory. No error if the directory already exists.
        /// </summary>
        public void CreateDirectory(string path)
        {
            lock (_lock)
            {
                Directory.CreateDirectory(path);
            }
        }

        /// <summary>
        /// Asynchronously attempt to create a directory. No error if the directory already exists.
        /// </summary>
        public async ValueTask CreateDirectoryAsync(string path, CancellationToken cancellationToken = default)
        {
            if (_holdsLock.Value)
            {
                CreateDirectory_UnderLock(path);
            }
            else
            {
                try
                {
                    using (await _asyncLock.LockAsync("SFD", cancellationToken).ConfigureAwait(false))
                    {
                        _holdsLock.Value = true;
                        CreateDirectory_UnderLock(path);
                    }
                }
                finally
                {
                    _holdsLock.Value = false;
                }
            }
        }

        /// <summary>
        /// Attempt to create a directory. No error if the directory already exists.
        /// <para>WARNING: Caller must have lock on <see cref="_asyncLock"/></para>
        /// </summary>
        private static void CreateDirectory_UnderLock(string path)
        {
            Directory.CreateDirectory(path);
        }

        /// <summary>
        /// Rename a file, including its path
        /// </summary>
        private static bool Move(string oldPath, string newPath)
        {
            lock (_lock)
            {
                for (var i = 0; i < RetryLimit; i++)
                {
                    try
                    {
                        File.Move(oldPath, newPath);
                        return true;
                    }
                    catch
                    {
                        Thread.Sleep(i * 100);
                    }
                }
            }
            return false;
        }

        /// <summary>
        /// Asynchronously rename a file, including its path
        /// </summary>
        private async Task<bool> MoveAsync(string oldPath, string newPath, CancellationToken cancellationToken = default)
        {
            if (_holdsLock.Value)
            {
                return Move_UnderLock(oldPath, newPath, cancellationToken);
            }
            else
            {
                try
                {
                    using (await _asyncLock.LockAsync("SFD", cancellationToken).ConfigureAwait(false))
                    {
                        _holdsLock.Value = true;
                        return Move_UnderLock(oldPath, newPath, cancellationToken);
                    }
                }
                finally
                {
                    _holdsLock.Value = false;
                }
            }
        }

        /// <summary>
        /// Rename a file, including its path
        /// <para>WARNING: Call must already have a lock on <see cref="_asyncLock"/>.</para>
        /// </summary>
        private bool Move_UnderLock(string oldPath, string newPath, CancellationToken cancellationToken = default)
        {
            string oldFileName = oldPath[oldPath.LastIndexOf('\\')..];
            string newFileName = newPath[newPath.LastIndexOf('\\')..];
            for (var i = 0; i < RetryLimit; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    File.Move(oldPath, newPath);
                    return true;
                }
                catch when (i < RetryLimit - 1)
                {
                    Thread.Sleep(i * 100);
                }
            }
            _logger.LogError("FAILED to MoveAsync file {OldFileName} to {NewFileName} after {RetryLimit} attempts", oldFileName, newFileName, RetryLimit);
            return false;
        }

        /// <summary>
        /// Open a transaction log file as a stream.
        /// </summary>
        public IFileStream OpenTransactionLog(string path, int bufferLength)
        {
            lock (_lock)
            {
                var stream = new FileStream(path,
                    FileMode.Append,
                    FileAccess.Write,
                    FileShare.None,
                    bufferLength,
                    FileOptions.SequentialScan | FileOptions.WriteThrough);

                return new FileStreamWrapper(stream);
            }
        }

        /// <summary>
        /// Asynchronously open a transaction log file as a stream
        /// </summary>
        public async Task<IFileStream> OpenTransactionLogAsync(string path, int bufferLength, CancellationToken cancellationToken = default)
        {
            try
            {
                using (await _asyncLock.LockAsync("SFD", cancellationToken).ConfigureAwait(false))
                {
                    _holdsLock.Value = true;
                    var stream = new FileStream(path,
                            FileMode.Append,
                            FileAccess.Write,
                            FileShare.None,
                            bufferLength,
                            FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.WriteThrough);

                    return (IFileStream)new FileStreamWrapper(stream);
                }
            }
            finally
            {
                _holdsLock.Value = false;
            }
        }

        /// <summary>
        /// Open a data file for reading
        /// </summary>
        public IFileStream OpenReadStream(string path)
        {
            lock (_lock)
            {
                var stream = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite);
                return new FileStreamWrapper(stream);
            }
        }

        /// <summary>
        /// Asynchronously open a data file for reading
        /// </summary>
        public async Task<IFileStream> OpenReadStreamAsync(string path, CancellationToken cancellationToken = default)
        {
            FileStream stream;
            try
            {
                using (await _asyncLock.LockAsync("SFD", cancellationToken).ConfigureAwait(false))
                {
                    _holdsLock.Value = true;
                    stream = new FileStream(
                            path,
                            FileMode.OpenOrCreate,
                            FileAccess.Read,
                            FileShare.ReadWrite,
                            bufferSize: 0x10000,
                            FileOptions.Asynchronous | FileOptions.SequentialScan);

                    return new FileStreamWrapper(stream);
                }
            }
            finally
            {
                _holdsLock.Value = false;
            }
        }

        /// <summary>
        /// Open a data file for writing
        /// </summary>
        public IFileStream OpenWriteStream(string dataFilePath)
        {
            lock (_lock)
            {
                var stream = new FileStream(
                    dataFilePath,
                    FileMode.OpenOrCreate,
                    FileAccess.Write,
                    FileShare.ReadWrite,
                    0x10000,
                    FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.WriteThrough);

                SetPermissions.TryAllowReadWriteForAll(dataFilePath);
                return new FileStreamWrapper(stream);
            }
        }

        /// <summary>
        /// Asynchronously open a data file for writing
        /// </summary>
        public async Task<IFileStream> OpenWriteStreamAsync(string dataFilePath, CancellationToken cancellationToken = default)
        {
            FileStream stream;
            try
            {
                using (await _asyncLock.LockAsync("SFD", cancellationToken).ConfigureAwait(false))
                {
                    _holdsLock.Value = true;
                    stream = new FileStream(
                            dataFilePath,
                            FileMode.OpenOrCreate,
                            FileAccess.Write,
                            FileShare.ReadWrite,
                            0x10000,
                            FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.WriteThrough);

                    SetPermissions.TryAllowReadWriteForAll(dataFilePath, _options.SetFilePermissions);
                    return new FileStreamWrapper(stream);
                }
            }
            finally
            {
                _holdsLock.Value = false;
            }
        }

        /// <summary>
        /// Run a read action over a file by name.
        /// Access is optimised for sequential scanning.
        /// No file share is permitted.
        /// </summary>
        public bool AtomicRead(string path, Action<IBinaryReader> action)
        {
            for (int i = 1; i <= RetryLimit; i++)
            {
                try
                {
                    AtomicReadInternal(path, fileStream =>
                    {
                        var wrapper = new FileStreamWrapper(fileStream);
                        action(wrapper);
                    });
                    return true;
                }
                catch (UnrecoverableException)
                {
                    throw;
                }
                catch (Exception)
                {
                    if (i >= RetryLimit)
                    {
                        PersistentQueue.Log("Exceeded retry limit during read");
                        return false;
                    }

                    Thread.Sleep(i * 100);
                }
            }
            return false;
        }

        /// <summary>
        /// Asynchronously run a read action over a file by name.
        /// Access is optimised for sequential scanning.
        /// No file share is permitted.
        /// </summary>
        public async Task<bool> AtomicReadAsync(string path, Func<IBinaryReader, Task> action, CancellationToken cancellationToken = default)
        {
            for (int i = 1; i <= RetryLimit; i++)
            {
                try
                {
                    await AtomicReadInternalAsync(path, async (fileStream) =>
                    {
                        var wrapper = new FileStreamWrapper(fileStream);
                        await action(wrapper).ConfigureAwait(false);
                    }, cancellationToken).ConfigureAwait(false);

                    return true;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (UnrecoverableException)
                {
                    throw;
                }
                catch (Exception) when (i < RetryLimit && !cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(i * 100, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    if (i >= RetryLimit)
                    {
                        _logger.LogError(ex, "Exceeded retry limit during async read.");
                        return false;
                    }
                }
            }
            return false;
        }

        public void AtomicWrite(string path, Action<IBinaryWriter> action)
        {
            for (int i = 1; i <= RetryLimit; i++)
            {
                try
                {
                    AtomicWriteInternal(path, fileStream =>
                    {
                        var wrapper = new FileStreamWrapper(fileStream);
                        action(wrapper);
                    });
                    return;
                }
                catch (Exception) when (i >= RetryLimit)
                {
                    throw;
                }
                catch (Exception)
                {
                    Thread.Sleep(i * 100);
                }
            }
        }

        /// <summary>
        /// Asynchronously run a write action over a file by name.
        /// No file share is permitted.
        /// </summary>
        public async Task AtomicWriteAsync(string path, Func<IBinaryWriter, Task> action, CancellationToken cancellationToken = default)
        {
            for (int i = 1; i <= RetryLimit; i++)
            {
                try
                {
                    await AtomicWriteInternalAsync(path, async (fileStream) =>
                    {
                        var wrapper = new FileStreamWrapper(fileStream);
                        await action(wrapper).ConfigureAwait(false);
                    }, cancellationToken).ConfigureAwait(false);

                    return;
                }
                catch (Exception) when (i >= RetryLimit)
                {
                    throw;
                }
                catch (Exception) when (i < RetryLimit && !cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(i * 100, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Rethrow if we're cancelled
                    if (cancellationToken.IsCancellationRequested)
                        throw;
                }
            }
        }

        /// <summary>
		/// Run a read action over a file by name.
		/// Access is optimised for sequential scanning.
		/// No file share is permitted.
		/// </summary>
		/// <param name="path">File path to read</param>
		/// <param name="action">Action to consume file stream. You do not need to close the stream yourself.</param>
        private void AtomicReadInternal(string path, Action<FileStream> action)
        {
            lock (_lock)
            {
                if (FileExists(path + ".old_copy")) WaitDelete(path);

                using var stream = new FileStream(path,
                    FileMode.OpenOrCreate,
                    FileAccess.Read,
                    FileShare.ReadWrite,
                    0x10000,
                    FileOptions.SequentialScan);

                SetPermissions.TryAllowReadWriteForAll(path);
                action(stream);
            }
        }

        /// <summary>
		/// Asynchronously run a read action over a file by name.
		/// Access is optimised for sequential scanning.
		/// No file share is permitted.
		/// </summary>
		/// <param name="path">File path to read</param>
		/// <param name="action">Action to consume file stream. You do not need to close the stream yourself.</param>
        /// <param name="cancellationToken">Cancellation token</param>
        private async Task AtomicReadInternalAsync(string path, Func<FileStream, Task> action, CancellationToken cancellationToken)
        {
            var fileExists = await FileExistsAsync(path + ".old_copy", cancellationToken).ConfigureAwait(false);

            if (fileExists)
            {
                await WaitDeleteAsync(path, cancellationToken).ConfigureAwait(false);
            }

            FileStream? stream = null;

            try
            {
                try
                {
                    using (await _asyncLock.LockAsync("SFD", cancellationToken).ConfigureAwait(false))
                    {
                        _holdsLock.Value = true;
                        stream = new FileStream(path,
                            FileMode.OpenOrCreate,
                            FileAccess.Read,
                            FileShare.ReadWrite,
                            0x10000,
                            FileOptions.Asynchronous | FileOptions.SequentialScan);

                        SetPermissions.TryAllowReadWriteForAll(path, _options.SetFilePermissions);
                    }
                }
                finally
                {
                    _holdsLock.Value = false;
                }

                await action(stream!).ConfigureAwait(false);
            }
            finally
            {
                if (stream != null)
                {
                    await stream.DisposeAsync().ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Run a write action to a file.
        /// This will always rewrite the file (no appending).
        /// </summary>
        /// <param name="path">File path to write</param>
        /// <param name="action">Action to write into file stream. You do not need to close the stream yourself.</param>
        private void AtomicWriteInternal(string path, Action<FileStream> action)
        {
            lock (_lock)
            {
                // if the old copy file exists, this means that we have
                // a previous corrupt write, so we will not overwrite it, but 
                // rather overwrite the current file and keep it as our backup.
                if (FileExists(path) && !FileExists(path + ".old_copy"))
                    Move(path, path + ".old_copy");

                var dir = Path.GetDirectoryName(path);
                if (dir is not null && !DirectoryExists(dir)) CreateDirectory(dir);

                using var stream = new FileStream(path,
                    FileMode.Create,
                    FileAccess.Write,
                    FileShare.ReadWrite,
                    0x10000,
                    FileOptions.WriteThrough | FileOptions.SequentialScan);

                SetPermissions.TryAllowReadWriteForAll(path);
                action(stream);
                HardFlush(stream);

                WaitDelete(path + ".old_copy");
            }
        }

        /// <summary>
        /// Asynchronously run a write action to a file.
        /// This will always rewrite the file (no appending).
        /// </summary>
        /// <param name="path">File path to write</param>
        /// <param name="action">Action to write into file stream. You do not need to close the stream yourself.</param>
        /// <param name="cancellationToken">Cancellation token</param>
        private async Task AtomicWriteInternalAsync(string path, Func<FileStream, Task> action, CancellationToken cancellationToken)
        {
            string oldCopyPath = path + ".old_copy";
            string? dirPath = Path.GetDirectoryName(path);
            bool needsBackup = false;
            bool dirExists = false;
            FileStream? stream = null;

            // Check if we need to create a backup first
            needsBackup = await FileExistsAsync(path, cancellationToken) &&
                        !(await FileExistsAsync(oldCopyPath, cancellationToken));

            if (dirPath != null)
            {
                dirExists = await DirectoryExistsAsync(dirPath, cancellationToken);
            }

            try
            {
                using (await _asyncLock.LockAsync("SFD", cancellationToken).ConfigureAwait(false))
                {
                    _holdsLock.Value = true;

                    // Create backup if needed
                    if (needsBackup)
                    {
                        await MoveAsync(path, oldCopyPath, cancellationToken).ConfigureAwait(false);
                    }

                    if (dirPath != null && !dirExists)
                    {
                            await CreateDirectoryAsync(dirPath, cancellationToken).ConfigureAwait(false);
                    }

                    // Open stream for writing
                    try
                    {
                        stream = new FileStream(path,
                            FileMode.Create,
                            FileAccess.Write,
                            FileShare.ReadWrite,
                            0x10000,
                            FileOptions.Asynchronous | FileOptions.WriteThrough | FileOptions.SequentialScan);

                        SetPermissions.TryAllowReadWriteForAll(path, _options.SetFilePermissions);

                        // Execute the write action
                        await action(stream!).ConfigureAwait(false);

                        // Ensure data is flushed
                        await HardFlushAsync(stream!, cancellationToken).ConfigureAwait(false);
                    }
                    finally
                    {
                        if (stream != null)
                        {
                            await stream.DisposeAsync().ConfigureAwait(false);
                        }
                    }
                }
            }
            finally
            {
                _holdsLock.Value = false;
            }

            // Clean up old backup
            await WaitDeleteAsync(path + ".old_copy", cancellationToken).ConfigureAwait(false);

        }

        /// <summary>
        /// Flush a stream, checking to see if its a file -- in which case it will ask for a flush-to-disk.
        /// </summary>
        private static void HardFlush(Stream? stream)
        {
            if (stream == null) return;
            if (stream is FileStream fs) fs.Flush(true);
            stream.Flush();
        }

        /// <summary>
        /// Asynchronously flush a stream, checking to see if its a file -- in which case it will ask for a flush-to-disk.
        /// </summary>
        private static async Task HardFlushAsync(Stream stream, CancellationToken cancellationToken)
        {
            if (stream == null) return;
            if (stream is FileStream fs)
            {
                // FileStream.FlushAsync doesn't support flushToDisk parameter, so we use Task.Run for the synchronous call
                await Task.Run(() => fs.Flush(true), cancellationToken);
            }
            else
            {
                await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        private void WaitDelete(string s)
        {
            for (var i = 0; i < RetryLimit; i++)
            {
                try
                {
                    lock (_lock)
                    {
                        PrepareDelete(s);
                        Finalise();
                    }

                    return;
                }
                catch
                {
                    Thread.Sleep(100);
                }
            }
        }

        /// <summary>
        /// Asynchronously delete a file with retries
        /// </summary>
        private async Task WaitDeleteAsync(string path, CancellationToken cancellationToken)
        {
            for (var i = 0; i < RetryLimit; i++)
            {
                try
                {
                    await WaitDeleteInternalAsync(path, cancellationToken).ConfigureAwait(false);
                    return;
                }
                catch when (i < RetryLimit - 1 && !cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(100, cancellationToken);
                }
            }
        }

        /// <summary>
        /// Asynchronously delete a file with retries
        /// </summary>
        private async Task WaitDeleteInternalAsync(string path, CancellationToken cancellationToken)
        {
            // lock is not held here
            await PrepareDeleteAsync(path, cancellationToken).ConfigureAwait(false);
            await FinaliseAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Strict mode exceptions that can't be retried
    /// </summary>
    public class UnrecoverableException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the UnrecoverableException class
        /// </summary>
        public UnrecoverableException()
            : base("An unrecoverable error occurred during file operation")
        {
        }

        /// <summary>
        /// Initializes a new instance of the UnrecoverableException class with a specified error message
        /// </summary>
        public UnrecoverableException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the UnrecoverableException class with a specified error message
        /// and a reference to the inner exception that is the cause of this exception
        /// </summary>
        public UnrecoverableException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}