// -----------------------------------------------------------------------
// <copyright file="SerializationStrategyInFactorySessionTests.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Tests
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using ModernDiskQueue;
    using ModernDiskQueue.Implementation;
    using ModernDiskQueue.Implementation.Interfaces;
    using NSubstitute;
    using NUnit.Framework;

    [TestFixture]
    public class SerializationStrategyInFactorySessionTests
    {
        private ILoggerFactory _mockLoggerFactory;
        private IFileDriver _mockFileDriver;
        private ISerializationStrategy<string> _mockStrategy;
        private readonly string _testPath = "test_path";

        [SetUp]
        public void Setup()
        {
            _mockLoggerFactory = Substitute.For<ILoggerFactory>();
            _mockLoggerFactory.CreateLogger(Arg.Any<string>()).Returns(Substitute.For<ILogger>());
            _mockFileDriver = Substitute.For<IFileDriver>();
            _mockStrategy = Substitute.For<ISerializationStrategy<string>>();
        }

        [Test]
        public async Task OpenSessionAsync_WithSerializationStrategy_CreatesSessionWithStrategy()
        {
            // Arrange
            var options = new ModernDiskQueueOptions();
            var queueImpl = new PersistentQueueImpl<string>(
            _mockLoggerFactory,
            _testPath,
            1024,
            true,
            true,
            options,
            _mockFileDriver);

            _mockFileDriver
                .OpenWriteStreamAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(Substitute.For<IFileStream>());

            // Act
            var session = await queueImpl.OpenSessionAsync(_mockStrategy);

            // Assert
            Assert.That(session, Is.Not.Null);
            Assert.That(session, Is.TypeOf<PersistentQueueSession<string>>());
            Assert.That(session.SerializationStrategy, Is.SameAs(_mockStrategy));
        }

        [Test]
        public async Task OpenSessionAsync_WithNoSerializationStrategy_CreatesSessionWithDefaultStrategy()
        {
            // Arrange
            var options = new ModernDiskQueueOptions();
            var queueImpl = new PersistentQueueImpl<string>(
            _mockLoggerFactory,
            _testPath,
            1024,
            true,
            true,
            options,
            _mockFileDriver);

            _mockFileDriver
                .OpenWriteStreamAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(Substitute.For<IFileStream>());

            // Act
            var session = await queueImpl.OpenSessionAsync();

            // Assert
            Assert.That(session, Is.Not.Null);
            Assert.That(session, Is.TypeOf<PersistentQueueSession<string>>());
            Assert.That(session.SerializationStrategy, Is.TypeOf<DefaultSerializationStrategy<string>>());
        }
    }
}