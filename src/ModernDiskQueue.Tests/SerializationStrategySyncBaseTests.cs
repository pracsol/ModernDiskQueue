// -----------------------------------------------------------------------
// <copyright file="SerializationStrategySyncBaseTests.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Tests
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using NUnit.Framework;

    [TestFixture]
    public class SerializationStrategyBaseTestsSync
    {
        [Test]
        public async Task SerializeAsync_CallsSerialize()
        {
            // Arrange
            var strategy = new TestSyncSerializationStrategy();
            var testString = "Hello World";
            var expected = System.Text.Encoding.UTF8.GetBytes(testString);

            // Act
            var result = await strategy.SerializeAsync(testString);

            // Assert
            Assert.That(result, Is.EqualTo(expected));
        }

        [Test]
        public async Task DeserializeAsync_CallsDeserialize()
        {
            // Arrange
            var strategy = new TestSyncSerializationStrategy();
            var testString = "Hello World";
            var bytes = System.Text.Encoding.UTF8.GetBytes(testString);

            // Act
            var result = await strategy.DeserializeAsync(bytes);

            // Assert
            Assert.That(result, Is.EqualTo(testString));
        }

        [Test]
        public async Task SerializeAsync_RespectsCancellationToken()
        {
            // Arrange
            var strategy = new TestSyncSerializationStrategy();
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
            var strategy = new TestSyncSerializationStrategy();
            var cts = new CancellationTokenSource();
            cts.Cancel();
            var bytes = System.Text.Encoding.UTF8.GetBytes("test");

            // Act & Assert
            var ex = Assert.ThrowsAsync<OperationCanceledException>(
            async () => await strategy.DeserializeAsync(bytes, cts.Token));
            Assert.That(ex, Is.Not.Null);
        }

        private class TestSyncSerializationStrategy : SyncSerializationStrategyBase<string>
        {
            public override string? Deserialize(byte[]? bytes)
            {
                if (bytes == null)
                {
                    return null;
                }

                return System.Text.Encoding.UTF8.GetString(bytes);
            }

            public override byte[]? Serialize(string? obj)
            {
                if (obj == null)
                {
                    return null;
                }

                return System.Text.Encoding.UTF8.GetBytes(obj);
            }
        }
    }
}