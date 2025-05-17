// -----------------------------------------------------------------------
// <copyright file="SerializationStrategyAsyncBaseTests.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Tests
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using NSubstitute;
    using NUnit.Framework;

    [TestFixture]
    public class SerializationStrategyAsyncBaseTests
    {
        [Test]
        public async Task SerializeAsync_ReturnsExpectedByteArray()
        {
            // Arrange
            var strategy = new TestAsyncSerializationStrategy();
            var testString = "Hello World";
            var expected = System.Text.Encoding.UTF8.GetBytes(testString);

            // Act
            var result = await strategy.SerializeAsync(testString);

            // Assert
            Assert.That(result, Is.EqualTo(expected));
        }

        [Test]
        public async Task DeserializeAsync_ReturnsExpectedString()
        {
            // Arrange
            var strategy = new TestAsyncSerializationStrategy();
            var testString = "Hello World";
            var bytes = System.Text.Encoding.UTF8.GetBytes(testString);

            // Act
            var result = await strategy.DeserializeAsync(bytes);

            // Assert
            Assert.That(result, Is.EqualTo(testString));
        }

        [Test]
        public void Serialize_CallsSerializeAsync()
        {
            // Arrange
            var strategy = new TestAsyncSerializationStrategy();
            var testString = "Hello World";
            var expected = System.Text.Encoding.UTF8.GetBytes(testString);

            // Act
            var result = strategy.Serialize(testString);

            // Assert
            Assert.That(result, Is.EqualTo(expected));
        }

        [Test]
        public void Deserialize_CallsDeserializeAsync()
        {
            // Arrange
            var strategy = new TestAsyncSerializationStrategy();
            var testString = "Hello World";
            var bytes = System.Text.Encoding.UTF8.GetBytes(testString);

            // Act
            var result = strategy.Deserialize(bytes);

            // Assert
            Assert.That(result, Is.EqualTo(testString));
        }

        [Test]
        public void Serialize_WrapsExceptionInInvalidOperationException()
        {
            // Arrange
            var strategy = new ThrowingAsyncSerializationStrategy();

            // Act & Assert
            var ex = Assert.Throws<InvalidOperationException>(() => strategy.Serialize("test"));
            Assert.That(ex.Message, Does.Contain("Error in synchronous fallback for async serialization"));
        }

        [Test]
        public void Deserialize_WrapsExceptionInInvalidOperationException()
        {
            // Arrange
            var strategy = new ThrowingAsyncSerializationStrategy();

            // Act & Assert
            var ex = Assert.Throws<InvalidOperationException>(() => strategy.Deserialize(new byte[] { 1, 2, 3 }));
            Assert.That(ex.Message, Does.Contain("Error in synchronous fallback for async deserialization"));
        }

        [Test]
        public async Task SerializeAsync_RespectsCancellationToken()
        {
            // Arrange
            var strategy = new TestAsyncSerializationStrategy();
            var cts = new CancellationTokenSource();
            cts.Cancel();

            // Act & Assert
            var ex = Assert.ThrowsAsync<OperationCanceledException>(
            async () => await strategy.SerializeAsync("test", cts.Token));
            Assert.That(ex, Is.Not.Null);
        }

        [Test]
        public async Task DeserializeAsync_RespectsCancellationToken()
        {
            // Arrange
            var strategy = new TestAsyncSerializationStrategy();
            var cts = new CancellationTokenSource();
            cts.Cancel();
            var bytes = System.Text.Encoding.UTF8.GetBytes("test");

            // Act & Assert
            var ex = Assert.ThrowsAsync<OperationCanceledException>(
            async () => await strategy.DeserializeAsync(bytes, cts.Token));
            Assert.That(ex, Is.Not.Null);
        }

        private class TestAsyncSerializationStrategy : AsyncSerializationStrategyBase<string>
        {
            public override ValueTask<string?> DeserializeAsync(byte[]? bytes, CancellationToken cancellationToken = default)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (bytes == null) return new ValueTask<string?>(default(string));
                return new ValueTask<string?>(System.Text.Encoding.UTF8.GetString(bytes));
            }

            public override ValueTask<byte[]?> SerializeAsync(string? obj, CancellationToken cancellationToken = default)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (obj == null) return new ValueTask<byte[]?>(default(byte[]));
                return new ValueTask<byte[]?>(System.Text.Encoding.UTF8.GetBytes(obj));
            }
        }

        private class ThrowingAsyncSerializationStrategy : AsyncSerializationStrategyBase<string>
        {
            public override ValueTask<string?> DeserializeAsync(byte[]? bytes, CancellationToken cancellationToken = default)
            {
                throw new Exception("Test exception");
            }

            public override ValueTask<byte[]?> SerializeAsync(string? obj, CancellationToken cancellationToken = default)
            {
                throw new Exception("Test exception");
            }
        }
    }
}
