namespace ModernDiskQueue.Tests
{
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Logging;
    using ModernDiskQueue.DependencyInjection;
    using ModernDiskQueue.Tests.Models;
    using NUnit.Framework;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    internal class DependencyInjectionTests
    {
        string QueuePath = "./DIConsumerTests";
        private ServiceProvider _serviceProvider;
        InMemoryLoggerProvider _logProvider = new();

        [SetUp]
        public void Setup()
        {
            var services = new ServiceCollection();
            _logProvider = new(); // if you don't reset this, you'll end up with duplicate entries in the log.

            // Add logging
            services.AddLogging(builder =>
            {
                builder.SetMinimumLevel(LogLevel.Trace);
                builder.AddConsole();
                builder.AddProvider(_logProvider);
            });

            // Add options configuration
            services.AddModernDiskQueue(options =>
            {
                options.AllowTruncatedEntries = true;
                options.SetFilePermissions = true;
                options.ParanoidFlushing = false;
                options.TrimTransactionLogOnDispose = false;
                options.FileTimeoutMilliseconds = 20000;
            });

            _serviceProvider = services.BuildServiceProvider();
        }

        [TearDown]
        public void Teardown()
        {
            if (_serviceProvider is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }

        [Test]
        public async Task CreateQueueFromFactory_OptionsAreSetOppositeDefault_QueueValuesShouldEqualOptions()
        {
            // Arrange
            var factory = _serviceProvider.GetRequiredService<IPersistentQueueFactory>();

            // Act
            await using var queue = await factory.CreateAsync(QueuePath);

            // Assert
            Assert.That(queue.Internals.AllowTruncatedEntries, Is.True, "AllowTruncatedEntries should match the configured option.");
            Assert.That(queue.Internals.SetFilePermissions, Is.True, "SetFilePermissions should match the configured option.");
            Assert.That(queue.Internals.ParanoidFlushing, Is.False, "ParanoidFlushing should match the configured option.");
            Assert.That(queue.Internals.TrimTransactionLogOnDispose, Is.False, "TrimTransactionLogOnDispose should match the configured option.");
            Assert.That(queue.Internals.FileTimeoutMilliseconds, Is.EqualTo(20000), "FileTimeoutMilliseconds should match the configured option.");
        }

        [Test]
        public async Task CreateQueueFromFactory_QueueCreatedWithoutSpecifyingOptions_DefaultOptionsShouldMatch()
        {
            // Arrange
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.SetMinimumLevel(LogLevel.Information);
                builder.AddConsole();
            });
            var factory = new PersistentQueueFactory(loggerFactory, new ModernDiskQueueOptions());

            // Act
            await using var queue = await factory.CreateAsync(QueuePath);

            // Assert
            Assert.That(queue.Internals.AllowTruncatedEntries, Is.False, "AllowTruncatedEntries should match the default option.");
            Assert.That(queue.Internals.SetFilePermissions, Is.False, "SetFilePermissions should match the default option.");
            Assert.That(queue.Internals.ParanoidFlushing, Is.True, "ParanoidFlushing should match the default option.");
            Assert.That(queue.Internals.TrimTransactionLogOnDispose, Is.True, "TrimTransactionLogOnDispose should match the default option.");
            Assert.That(queue.Internals.FileTimeoutMilliseconds, Is.EqualTo(10000), "FileTimeoutMilliseconds should match the default option.");
        }

        [Test]
        public async Task CreateQueueFromFactory_EnqueueObject_CanDequeueObject()
        {
            // Arrange
            var factory = _serviceProvider.GetRequiredService<IPersistentQueueFactory>();
            byte[] input = [1,2,3,4];
            byte[]? output;

            // Act
            await using (var queue = await factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    await session.EnqueueAsync(input);
                    await session.FlushAsync();
                    output = await session.DequeueAsync();
                }
            }

            // Assert
            Assert.That(output, Is.Not.Null, "Dequeue should return a non-null value.");
            Assert.That(output, Is.EqualTo(input), "Dequeue should return the same value that was enqueued.");
        }

        [Test]
        public async Task CreateQueueFromFactory_ChangeDefaultSettingsBeforeQueueCreation_OptionsOverrideDefaultSettings()
        {
            // Arrange
            PersistentQueue.DefaultSettings.FileTimeoutMilliseconds = 9999;
            int returnedTimeout = 0;
            var factory = _serviceProvider.GetRequiredService<IPersistentQueueFactory>();

            // Act
            await using (var queue = await factory.CreateAsync(QueuePath))
            {
                returnedTimeout = queue.Internals.FileTimeoutMilliseconds;
            }

            // Assert
            Assert.That(returnedTimeout, Is.EqualTo(20000), "Default setting should be overwritten by factory options.");
        }

        [Test]
        public async Task CreateQueueFromFactory_ChangeDefaultSettingsAfterQueueCreation_NoChangeToInternals()
        {
            // Arrange
            int returnedTimeout = 0;
            var factory = _serviceProvider.GetRequiredService<IPersistentQueueFactory>();

            // Act
            await using (var queue = await factory.CreateAsync(QueuePath))
            {
                PersistentQueue.DefaultSettings.FileTimeoutMilliseconds = 9999;
                returnedTimeout = queue.Internals.FileTimeoutMilliseconds;
            }

            // Assert
            Assert.That(returnedTimeout, Is.EqualTo(20000), "Default setting should be overwritten by factory options.");
        }

        [Test]
        public async Task CreateQueueFromFactory_ChangeInternalsAfterQueueCreation_ValueChanged()
        {
            // Arrange
            int originalTimeout = 0;
            int changedTimeout = 0;
            var factory = _serviceProvider.GetRequiredService<IPersistentQueueFactory>();

            // Act
            await using (var queue = await factory.CreateAsync(QueuePath))
            {
                originalTimeout = queue.Internals.FileTimeoutMilliseconds;
                queue.Internals.FileTimeoutMilliseconds = 9999;
                changedTimeout = queue.Internals.FileTimeoutMilliseconds;

            }

            // Assert
            Assert.That(originalTimeout, Is.EqualTo(20000), "Initial timeout value at queue creation should equal options set at factory instantiation.");
            Assert.That(changedTimeout, Is.EqualTo(9999), "Changed timeout value should be current value.");
        }

        [Test]
        public async Task CreateQueueFromFactory_InvokeCallToFileDriver_LoggingMessagesUseDiContainerLogger()
        {
            // Arrange
            int originalTimeout = 0;
            int changedTimeout = 0;
            // Arrange
            var factory = _serviceProvider.GetRequiredService<IPersistentQueueFactory>();

            // Act
            await using (var queue = await factory.CreateAsync(QueuePath))
            {

            }
            await using (var queue = await factory.CreateAsync<string>(QueuePath))
            {

            }
            // Assert

            var logsFromFactory = _logProvider.GetMessages("PersistentQueueFactory");
            var logs = _logProvider.LogEntries;

            int queueCreationMessageCount = logs.Count(entry =>
                entry.Level == LogLevel.Information &&
                entry.Category.Contains("ModernDiskQueue.PersistentQueueFactory") &&
                entry.Message.Contains("creating queue at"));

            Assert.That(queueCreationMessageCount, Is.EqualTo(2),
                "Expected two informational entries from factory when queue is created");

            Assert.That(logs.Any(entry =>
                entry.Level == LogLevel.Trace &&
                entry.Category.Contains("ModernDiskQueue.Implementation.StandardFileDriver") &&
                entry.Message.Contains("created lock file")),
                "Expected trace/verbose entry from IFileDriver when lock file created.");

            Assert.That(logs.Any(entry =>
                entry.Level == LogLevel.Trace &&
                entry.Category.Contains("ModernDiskQueue.Implementation.StandardFileDriver") &&
                entry.Message.Contains("released lock file")),
                "Expected trace/verbose entry from IFileDriver when lock file released.");
        }
        
    }
}
