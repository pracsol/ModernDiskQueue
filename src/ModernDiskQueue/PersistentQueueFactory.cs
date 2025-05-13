
namespace ModernDiskQueue
{
    using System.Diagnostics.CodeAnalysis;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.Abstractions;
    using Microsoft.Extensions.Options;
    using ModernDiskQueue.Implementation;
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.DependencyInjection;
    using ModernDiskQueue.PublicInterfaces;

    /// <summary>
    /// Factory for creating <see cref="PersistentQueue{T}"/> instances.
    /// </summary>
    public class PersistentQueueFactory : IPersistentQueueFactory
    {
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger<PersistentQueueFactory> _logger;
        private readonly ModernDiskQueueOptions _options;
        //private readonly IFileDriver _fileDriver;
        private readonly Lazy<StandardFileDriver> _lazyFileDriver;

        /// <summary>
        /// Create a new instance of <see cref="PersistentQueueFactory"/>.
        /// </summary>
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(Logger<>))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(LoggerFactory))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(ILoggerFactory))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(ILogger<>))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, "Microsoft.Extensions.Logging.ILogger`1", "Microsoft.Extensions.Logging.Abstractions")]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(ModernDiskQueueOptions))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(StandardFileDriver))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(PersistentQueueFactory))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(IPersistentQueueFactory))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(IFileDriver))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(IServiceCollection))]
        public PersistentQueueFactory()
            : this(NullLoggerFactory.Instance, new ModernDiskQueueOptions()) { }

        /// <summary>
        /// Create a new instance of <see cref="PersistentQueueFactory"/>.
        /// </summary>
        /// <param name="loggerFactory">Implementation of <see cref="ILoggerFactory"/></param>
        [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(Logger<>))]
        public PersistentQueueFactory(ILoggerFactory loggerFactory)
            : this(loggerFactory, Options.Create(new ModernDiskQueueOptions())) { }

        /// <summary>
        /// Create a new instance of <see cref="PersistentQueueFactory"/>.
        /// </summary>
        /// <param name="options">Default options.</param>
        [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(Logger<>))]
        public PersistentQueueFactory(ModernDiskQueueOptions options)
            : this(NullLoggerFactory.Instance, Options.Create(options)) { }

        /// <summary>
        /// Create a new instance of <see cref="PersistentQueueFactory"/>.
        /// </summary>
        /// <param name="loggerFactory">Implementation of <see cref="ILoggerFactory"/></param>
        /// <param name="options">Default options.</param>
        [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(Logger<>))]
        public PersistentQueueFactory(ILoggerFactory loggerFactory, ModernDiskQueueOptions options)
            : this(loggerFactory, Options.Create(options)) { }

        /// <summary>
        /// Create a new instance of <see cref="PersistentQueueFactory"/>.
        /// </summary>
        /// <param name="loggerFactory">Implementation of <see cref="ILoggerFactory"/></param>
        /// <param name="options">Default options.</param>
        public PersistentQueueFactory(ILoggerFactory loggerFactory, IOptions<ModernDiskQueueOptions> options)
        {
            if (options == null)
            {
                throw new ArgumentNullException(nameof(options));
            }
            _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            _logger = loggerFactory?.CreateLogger<PersistentQueueFactory>() ?? NullLogger<PersistentQueueFactory>.Instance;
            _options = options.Value ?? new();
            _lazyFileDriver = new Lazy<StandardFileDriver>(() =>
                new StandardFileDriver(_loggerFactory, options));
            //_fileDriver = fileDriver ?? throw new ArgumentNullException(nameof(fileDriver));
        }

        // Expose the lazily initialized driver through a property
        private StandardFileDriver FileDriver => _lazyFileDriver.Value;

        /// <inheritdoc/>
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)]
        public async Task<PersistentQueue> CreateAsync(string storagePath, CancellationToken cancellationToken = default)
        {
            return await CreateAsync(storagePath, Constants._32Megabytes, true, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)]
        public async Task<PersistentQueue> CreateAsync(string storagePath, int maxSize, bool throwOnConflict = true, CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("Thread {ThreadId} creating queue at {storagePath} with options: {Options}", Environment.CurrentManagedThreadId, storagePath, _options.ToString());
            SetGlobalDefaultsFromFactoryOptions(_options);
            var queue = new PersistentQueueImpl(_loggerFactory, storagePath, maxSize, throwOnConflict, true, _options, FileDriver);
            await queue.InitializeAsync(cancellationToken).ConfigureAwait(false);
            return new PersistentQueue(_loggerFactory, queue);
        }

        /// <inheritdoc/>
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)]
        public async Task<PersistentQueue<T>> CreateAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] T>(string storagePath, CancellationToken cancellationToken = default)
        {
            return await CreateAsync<T>(storagePath, Constants._32Megabytes, true, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)]
        public async Task<PersistentQueue<T>> CreateAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] T>(string storagePath, int maxSize, bool throwOnConflict = true, CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("Thread {ThreadId} creating queue at {storagePath} with options: {Options}", Environment.CurrentManagedThreadId, storagePath, _options.ToString());
            SetGlobalDefaultsFromFactoryOptions(_options);
            var queue = new PersistentQueueImpl<T>(_loggerFactory, storagePath, maxSize, throwOnConflict, true, _options, FileDriver);
            await queue.InitializeAsync(cancellationToken).ConfigureAwait(false);
            return new PersistentQueue<T>(_loggerFactory, queue);
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
            var random = new Random();
            int retryCount = 0;

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
                        // Exponential backoff with jitter
                        retryCount++;
                        // Base delay increases exponentially (50ms * 2^retryCount)
                        double baseDelay = Math.Min(50 * Math.Pow(2, retryCount), 1000); // Cap at 1 second
                        // Add jitter of ±25%
                        int jitterPercent = random.Next(-25, 26);
                        int delayMs = (int)(baseDelay * (1 + jitterPercent / 100.0));

                        _logger.LogTrace("Queue locked. Thread {ThreadID} Retrying queue access in {DelayMs}ms", Environment.CurrentManagedThreadId, delayMs);
                        await Task.Delay(50, cancellationToken).ConfigureAwait(false);
                    }
                } while (sw.Elapsed < maxWait);
            }
            finally
            {
                sw.Stop();
            }
            throw new TimeoutException($"Could not acquire a lock on '{lockName}'. Directed to wait {maxWait.TotalMilliseconds}ms and really waited {sw.ElapsedMilliseconds}ms.");
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
