
namespace ModernDiskQueue
{
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.Abstractions;
    using Microsoft.Extensions.Options;
    using ModernDiskQueue.Implementation;
    using ModernDiskQueue.PublicInterfaces;
    using System;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    /// <summary>
    /// Factory for creating <see cref="PersistentQueue{T}"/> instances.
    /// </summary>
    public class PersistentQueueFactory : IPersistentQueueFactory
    {
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger<PersistentQueueFactory> _logger;
        private readonly ModernDiskQueueOptions _options;
        private readonly IFileDriver _fileDriver;

        /// <summary>
        /// Create a new instance of <see cref="PersistentQueueFactory"/>.
        /// </summary>
        public PersistentQueueFactory()
            : this(NullLoggerFactory.Instance, new ModernDiskQueueOptions()) { }

        /// <summary>
        /// Create a new instance of <see cref="PersistentQueueFactory"/>.
        /// </summary>
        /// <param name="loggerFactory">Implementation of <see cref="ILoggerFactory"/></param>
        public PersistentQueueFactory(ILoggerFactory loggerFactory)
            : this(loggerFactory, Options.Create(new ModernDiskQueueOptions()), new StandardFileDriver()) { }

        /// <summary>
        /// Create a new instance of <see cref="PersistentQueueFactory"/>.
        /// </summary>
        /// <param name="options">Default options.</param>
        public PersistentQueueFactory(ModernDiskQueueOptions options)
            : this(NullLoggerFactory.Instance, Options.Create(options), new StandardFileDriver()) { }

        /// <summary>
        /// Create a new instance of <see cref="PersistentQueueFactory"/>.
        /// </summary>
        /// <param name="loggerFactory">Implementation of <see cref="ILoggerFactory"/></param>
        /// <param name="options">Default options.</param>
        public PersistentQueueFactory(ILoggerFactory loggerFactory, ModernDiskQueueOptions options)
            : this(loggerFactory, Options.Create(options), new StandardFileDriver()) { }

        /// <summary>
        /// Create a new instance of <see cref="PersistentQueueFactory"/>.
        /// </summary>
        /// <param name="loggerFactory">Implementation of <see cref="ILoggerFactory"/></param>
        /// <param name="options">Default options.</param>
        /// <param name="fileDriver"><see cref="IFileDriver"/> implementation.</param>
        public PersistentQueueFactory(ILoggerFactory loggerFactory, IOptions<ModernDiskQueueOptions> options, IFileDriver fileDriver)
        {
            _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            _logger = loggerFactory?.CreateLogger<PersistentQueueFactory>() ?? NullLogger<PersistentQueueFactory>.Instance;
            _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
            _fileDriver = fileDriver ?? throw new ArgumentNullException(nameof(fileDriver));
        }

        /// <inheritdoc/>
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)]
        public async Task<PersistentQueue> CreateAsync(string storagePath, CancellationToken cancellationToken = default)
        {
            return await CreateAsync(storagePath, Constants._32Megabytes, true, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)]
        public async Task<PersistentQueue> CreateAsync(string storagePath, int maxSize, bool throwOnConflict = true, CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("Creating queue at {storagePath} with options: {Options}", storagePath, _options);
            SetGlobalDefaultsFromFactoryOptions(_options);
            var queue = new PersistentQueueImpl(_loggerFactory, storagePath, maxSize, throwOnConflict, true, _options, _fileDriver);
            await queue.InitializeAsync(cancellationToken).ConfigureAwait(false);
            return new PersistentQueue(queue);
        }

        /// <inheritdoc/>
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)]
        public async Task<PersistentQueue<T>> CreateAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] T>(string storagePath, CancellationToken cancellationToken = default)
        {
            return await CreateAsync<T>(storagePath, Constants._32Megabytes, true, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)]
        public async Task<PersistentQueue<T>> CreateAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] T>(string storagePath, int maxSize, bool throwOnConflict = true, CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("Creating queue at {storagePath} with options: {Options}", storagePath, _options);
            SetGlobalDefaultsFromFactoryOptions(_options);
            var queue = new PersistentQueueImpl<T>(_loggerFactory, storagePath, Constants._32Megabytes, true, true, _options, _fileDriver);
            await queue.InitializeAsync(cancellationToken).ConfigureAwait(false);
            return new PersistentQueue<T>(queue);
        }

        /// <summary>
        /// Asynchronously waits a specified maximum time for exclusive access to a queue.
        /// The queue is opened with default max file size (32MiB) and conflicts set to throw exceptions.
        /// <para>If sharing storage between processes, the resulting queue should disposed
        /// as soon as possible.</para>
        /// <para>Throws a TimeoutException if the queue can't be locked in the specified time</para>
        /// </summary>
        /// <remarks>
        /// This class implements <see cref="IAsyncDisposable"/>. Always use <c>await using</c>
        /// instead of <c>using</c> with async methods to ensure proper asynchronous resource cleanup.
        /// </remarks>
        /// <param name="storagePath">Directory path for queue storage. This will be created if it doesn't already exist.</param>
        /// <param name="maxWait">If the storage path can't be locked within this time, a TimeoutException will be thrown.</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/> to monitor for cancellation requests.</param>
        /// <exception cref="TimeoutException">Lock on file could not be acquired</exception>
        public async Task<IPersistentQueue> WaitForAsync(string storagePath, TimeSpan maxWait, CancellationToken cancellationToken = default)
        {
            return await WaitForAsync(() => CreateAsync(storagePath, cancellationToken), maxWait, storagePath, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Asynchronously waits a specified maximum time for exclusive access to a queue.
        /// The queue is opened with default max file size (32MiB) and conflicts set to throw exceptions.
        /// <para>If sharing storage between processes, the resulting queue should disposed
        /// as soon as possible.</para>
        /// <para>Throws a TimeoutException if the queue can't be locked in the specified time</para>
        /// </summary>
        /// <remarks>
        /// This class implements <see cref="IAsyncDisposable"/>. Always use <c>await using</c>
        /// instead of <c>using</c> with async methods to ensure proper asynchronous resource cleanup.
        /// </remarks>
        /// <param name="storagePath">Directory path for queue storage. This will be created if it doesn't already exist.</param>
        /// <param name="maxSize">Maximum size in bytes for each storage file. Files will be rotated after reaching this limit.</param>
        /// <param name="throwOnConflict">When true, if data files are damaged, throw an InvalidOperationException. This will stop program flow.</param>
        /// <param name="maxWait">If the storage path can't be locked within this time, a TimeoutException will be thrown.</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/> to monitor for cancellation requests.</param>
        /// <exception cref="TimeoutException">Lock on file could not be acquired</exception>
        public async Task<IPersistentQueue> WaitForAsync(string storagePath, int maxSize, bool throwOnConflict, TimeSpan maxWait, CancellationToken cancellationToken = default)
        {
            return await WaitForAsync(() => CreateAsync(storagePath, maxSize, throwOnConflict, cancellationToken), maxWait, storagePath, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Wait a maximum time to open an exclusive session.
        /// The queue is opened with default max file size (32MiB) and conflicts set to throw exceptions.
        /// <para>If sharing storage between processes, the resulting queue should disposed
        /// as soon as possible.</para>
        /// <para>Throws a TimeoutException if the queue can't be locked in the specified time</para>
        /// </summary>
        /// <remarks>
        /// This class implements <see cref="IAsyncDisposable"/>. Always use <c>await using</c>
        /// instead of <c>using</c> with async methods to ensure proper asynchronous resource cleanup.
        /// </remarks>
        /// <typeparam name="T"></typeparam>
        /// <param name="storagePath"></param>
        /// <param name="maxWait"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<IPersistentQueue<T>> WaitForAsync<T>(string storagePath, TimeSpan maxWait, CancellationToken cancellationToken = default)
        {
            return await WaitForAsync(() => CreateAsync<T>(storagePath, cancellationToken), maxWait, storagePath, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Wait a maximum time to open an exclusive session.
        /// <para>If sharing storage between processes, the resulting queue should disposed
        /// as soon as possible.</para>
        /// <para>Throws a TimeoutException if the queue can't be locked in the specified time</para>
        /// </summary>
        /// <remarks>
        /// This class implements <see cref="IAsyncDisposable"/>. Always use <c>await using</c>
        /// instead of <c>using</c> with async methods to ensure proper asynchronous resource cleanup.
        /// </remarks>
        /// <typeparam name="T"></typeparam>
        /// <param name="storagePath"></param>
        /// <param name="maxSize"></param>
        /// <param name="throwOnConflict"></param>
        /// <param name="maxWait"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<IPersistentQueue<T>> WaitForAsync<T>(string storagePath, int maxSize, bool throwOnConflict, TimeSpan maxWait, CancellationToken cancellationToken = default)
        {
            return await WaitForAsync(() => CreateAsync<T>(storagePath, maxSize, throwOnConflict, cancellationToken), maxWait, storagePath, cancellationToken).ConfigureAwait(false);
        }

        private async Task<T> WaitForAsync<T>(Func<Task<T>> generator, TimeSpan maxWait, string lockName, CancellationToken cancellationToken = default)
        {
            var sw = new Stopwatch();
            try
            {
                sw.Start();
                do
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    try
                    {
                        return await generator().ConfigureAwait(false);
                    }
                    catch (DirectoryNotFoundException ex)
                    {
                        _logger.LogError("Error while waiting for queue: {Message}", ex.Message);
                        throw new Exception("Target storagePath does not exist or is not accessible");
                    }
                    catch (PlatformNotSupportedException ex)
                    {
                        _logger.LogError(ex, "Error while waiting for queue. Blocked by {ExceptionName}; {ErrorMessage}\n\n{StackTrace}", ex.GetType()?.Name, ex.Message, ex.StackTrace);
                        throw;
                    }
                    catch
                    {
                        await Task.Delay(50, cancellationToken).ConfigureAwait(false);
                    }
                } while (sw.Elapsed < maxWait);
            }
            finally
            {
                sw.Stop();
            }
            throw new TimeoutException($"Could not acquire a lock on '{lockName}' in the time specified");
        }

        private void SetGlobalDefaultsFromFactoryOptions(ModernDiskQueueOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));

            PersistentQueue.DefaultSettings.AllowTruncatedEntries = options.AllowTruncatedEntries;
            PersistentQueue.DefaultSettings.ParanoidFlushing = options.ParanoidFlushing;
            PersistentQueue.DefaultSettings.SetFilePermissions = options.SetFilePermissions;
            PersistentQueue.DefaultSettings.TrimTransactionLogOnDispose = options.TrimTransactionLogOnDispose;
            PersistentQueue.DefaultSettings.FileTimeoutMilliseconds = options.FileTimeoutMilliseconds;
        }
    }
}
