// -----------------------------------------------------------------------
// <copyright file="SerializationStrategyInSessionConstructorTests.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Tests
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using ModernDiskQueue;
    using ModernDiskQueue.Implementation;
    using ModernDiskQueue.Implementation.Interfaces;
    using NSubstitute;
    using NUnit.Framework;

    [TestFixture]
    public class SerializationStrategyInSessionConstructorTests
    {
        private ILoggerFactory _mockLoggerFactory;
        private IPersistentQueueImpl _mockQueue;
        private IFileStream _mockFileStream;
        private ISerializationStrategy<string> _mockStrategy;

        [SetUp]
        public void Setup()
        {
            _mockLoggerFactory = Substitute.For<ILoggerFactory>();
            _mockQueue = Substitute.For<IPersistentQueueImpl>();
            _mockFileStream = Substitute.For<IFileStream>();
            _mockStrategy = Substitute.For<ISerializationStrategy<string>>();
        }

        [Test]
        public void Constructor_WithSerializationStrategy_UsesProvidedStrategy()
        {
            // Arrange
            var customStrategy = _mockStrategy;

            // Act
            var session = new PersistentQueueSession<string>(
            _mockLoggerFactory,
            _mockQueue,
            _mockFileStream,
            4096,
            5000,
            customStrategy);

            // Assert
            Assert.That(session.SerializationStrategy, Is.SameAs(customStrategy));
        }

        [Test]
        public void Constructor_WithoutSerializationStrategy_UsesDefaultStrategy()
        {
            // Act
            var session = new PersistentQueueSession<string>(
            _mockLoggerFactory,
            _mockQueue,
            _mockFileStream,
            4096,
            5000);

            // Assert
            Assert.That(session.SerializationStrategy, Is.TypeOf<DefaultSerializationStrategy<string>>());
        }

        [Test]
        public void Enqueue_CallsSerializeOnStrategy()
        {
            // Arrange
            var testData = "Test data";
            var serializedData = System.Text.Encoding.UTF8.GetBytes(testData);

            _mockStrategy.Serialize(testData).Returns(serializedData);

            var session = new PersistentQueueSession<string>(
            _mockLoggerFactory,
            _mockQueue,
            _mockFileStream,
            4096,
            5000,
            _mockStrategy);

            // Act
            session.Enqueue(testData);

            // Assert
            _mockStrategy.Received(1).Serialize(testData);
        }

        [Test]
        public async Task EnqueueAsync_CallsSerializeAsyncOnStrategy()
        {
            // Arrange
            var testData = "Test data";
            var serializedData = System.Text.Encoding.UTF8.GetBytes(testData);

            _mockStrategy.SerializeAsync(testData, Arg.Any<CancellationToken>())
                .Returns(new ValueTask<byte[]?>(serializedData));

            var session = new PersistentQueueSession<string>(
            _mockLoggerFactory,
            _mockQueue,
            _mockFileStream,
            4096,
            5000,
            _mockStrategy);

            // Act
            await session.EnqueueAsync(testData);

            // Assert
            await _mockStrategy.Received(1).SerializeAsync(testData, Arg.Any<CancellationToken>());
        }
    }
}
