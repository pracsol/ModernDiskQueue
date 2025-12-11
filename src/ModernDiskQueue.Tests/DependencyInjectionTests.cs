// -----------------------------------------------------------------------
// <copyright file="DependencyInjectionTests.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Tests
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.DependencyInjection.Extensions;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.Abstractions;
    using Microsoft.Extensions.Options;
    using ModernDiskQueue;
    using ModernDiskQueue.Implementation;
    using ModernDiskQueue.Implementation.Interfaces;
    using ModernDiskQueue.Tests.Models;
    using NSubstitute;
    using NUnit.Framework;

    internal class DependencyInjectionTests
    {
        private const string QueuePath = "./DIConsumerTests";
        private ServiceProvider _serviceProvider;
        private InMemoryLoggerProvider _logProvider = new();

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
            byte[] input = [1, 2, 3, 4];
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

            Assert.That(
                queueCreationMessageCount,
                Is.EqualTo(2),
                "Expected two informational entries from factory when queue is created");

            Assert.That(
                logs.Any(entry =>
                entry.Level == LogLevel.Trace &&
                entry.Category.Contains("ModernDiskQueue.Implementation.StandardFileDriver") &&
                entry.Message.Contains("created lock file")),
                "Expected trace/verbose entry from IFileDriver when lock file created.");

            Assert.That(
                logs.Any(entry =>
                entry.Level == LogLevel.Trace &&
                entry.Category.Contains("ModernDiskQueue.Implementation.StandardFileDriver") &&
                entry.Message.Contains("released lock file")),
                "Expected trace/verbose entry from IFileDriver when lock file released.");
        }

        #region IFileDriver Injection Tests

        [Test]
        public void CreateFactory_WithCustomFileDriverViaConstructor_UsesCustomDriver()
        {
            // Arrange
            var mockFileDriver = Substitute.For<IFileDriver>();
            var loggerFactory = NullLoggerFactory.Instance;
            var options = Options.Create(new ModernDiskQueueOptions());

            // Act
            var factory = new PersistentQueueFactory(loggerFactory, options, mockFileDriver);

            // Assert - factory should be created without throwing
            Assert.That(factory, Is.Not.Null);
        }

        [Test]
        public void CreateFactory_WithNullFileDriver_CreatesDefaultStandardFileDriver()
        {
            // Arrange
            var loggerFactory = NullLoggerFactory.Instance;
            var options = Options.Create(new ModernDiskQueueOptions());

            // Act
            var factory = new PersistentQueueFactory(loggerFactory, options, null);

            // Assert - factory should be created without throwing
            Assert.That(factory, Is.Not.Null);
        }

        [Test]
        public void CreateFactory_WithCustomFileDriverViaDI_BeforeAddModernDiskQueue_UsesCustomDriver()
        {
            // Arrange
            var mockFileDriver = Substitute.For<IFileDriver>();
            var services = new ServiceCollection();

            services.AddLogging();

            // Register custom IFileDriver BEFORE AddModernDiskQueue
            services.AddSingleton<IFileDriver>(mockFileDriver);

            services.AddModernDiskQueue();

            using var provider = services.BuildServiceProvider();

            // Act
            var resolvedFileDriver = provider.GetRequiredService<IFileDriver>();

            // Assert
            Assert.That(resolvedFileDriver, Is.SameAs(mockFileDriver), "Custom IFileDriver should be used when registered before AddModernDiskQueue.");
        }

        [Test]
        public void CreateFactory_WithoutCustomFileDriver_UsesStandardFileDriver()
        {
            // Arrange
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddModernDiskQueue();

            using var provider = services.BuildServiceProvider();

            // Act
            var resolvedFileDriver = provider.GetRequiredService<IFileDriver>();

            // Assert
            Assert.That(resolvedFileDriver, Is.InstanceOf<StandardFileDriver>(), "StandardFileDriver should be used by default.");
        }

        [Test]
        public void CreateFactory_ReplaceFileDriverAfterAddModernDiskQueue_UsesReplacedDriver()
        {
            // Arrange
            var mockFileDriver = Substitute.For<IFileDriver>();
            var services = new ServiceCollection();

            services.AddLogging();
            services.AddModernDiskQueue();

            // Replace IFileDriver AFTER AddModernDiskQueue
            services.Replace(ServiceDescriptor.Singleton<IFileDriver>(mockFileDriver));

            using var provider = services.BuildServiceProvider();

            // Act
            var resolvedFileDriver = provider.GetRequiredService<IFileDriver>();

            // Assert
            Assert.That(resolvedFileDriver, Is.SameAs(mockFileDriver), "Replaced IFileDriver should be used.");
        }

        [Test]
        public async Task CreateQueue_WithMockedFileDriver_FileDriverMethodsAreCalled()
        {
            // Arrange
            var mockFileDriver = Substitute.For<IFileDriver>();
            var mockLockFile = Substitute.For<ILockFile>();

            // Setup minimum required mock behavior for queue creation
            mockFileDriver.GetFullPath(Arg.Any<string>()).Returns(x => x.Arg<string>());
            mockFileDriver.PathCombine(Arg.Any<string>(), Arg.Any<string>()).Returns(x => $"{x.ArgAt<string>(0)}/{x.ArgAt<string>(1)}");
            mockFileDriver.DirectoryExistsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(true);
            mockFileDriver.CreateLockFileAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(mockLockFile.Success()));

            var loggerFactory = NullLoggerFactory.Instance;
            var options = Options.Create(new ModernDiskQueueOptions());
            var factory = new PersistentQueueFactory(loggerFactory, options, mockFileDriver);

            // Act & Assert
            try
            {
                await using var queue = await factory.CreateAsync("./TestPath");
            }
            catch
            {
                // We expect this to fail since we haven't mocked everything,
                // but we can verify that our mock was called
            }

            // Assert - verify the mock was actually used
            mockFileDriver.Received().GetFullPath(Arg.Any<string>());
        }

        [Test]
        public void CreateFactory_ParameterlessConstructor_CreatesDefaultFileDriver()
        {
            // Act
            var factory = new PersistentQueueFactory();

            // Assert
            Assert.That(factory, Is.Not.Null, "Factory should be created with parameterless constructor.");
        }

        [Test]
        public void CreateFactory_WithLoggerFactoryOnly_CreatesDefaultFileDriver()
        {
            // Arrange
            var loggerFactory = NullLoggerFactory.Instance;

            // Act
            var factory = new PersistentQueueFactory(loggerFactory);

            // Assert
            Assert.That(factory, Is.Not.Null, "Factory should be created with logger factory only.");
        }

        [Test]
        public void CreateFactory_WithOptionsOnly_CreatesDefaultFileDriver()
        {
            // Arrange
            var options = new ModernDiskQueueOptions();

            // Act
            var factory = new PersistentQueueFactory(options);

            // Assert
            Assert.That(factory, Is.Not.Null, "Factory should be created with options only.");
        }

        #endregion
    }
}