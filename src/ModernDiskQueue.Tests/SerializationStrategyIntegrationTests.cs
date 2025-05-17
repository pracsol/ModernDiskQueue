// -----------------------------------------------------------------------
// <copyright file="SerializationStrategyIntegrationTests.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Tests
{
    using System;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using ModernDiskQueue;
    using NUnit.Framework;

    [TestFixture]
    public class SerializationStrategyIntegrationTests
    {
        [Test]
        public void JsonSyncSerializer_RoundTrip()
        {
            // Arrange
            var serializer = new JsonSyncSerializer();
            var testData = new TestData { Id = 123, Name = "Test Name" };

            // Act
            var serialized = serializer.Serialize(testData);
            var deserialized = serializer.Deserialize(serialized);

            // Assert
            Assert.That(deserialized, Is.Not.Null);
            Assert.That(deserialized!.Id, Is.EqualTo(123));
            Assert.That(deserialized.Name, Is.EqualTo("Test Name"));
        }

        [Test]
        public async Task JsonAsyncSerializer_RoundTrip()
        {
            // Arrange
            var serializer = new JsonAsyncSerializer();
            var testData = new TestData { Id = 123, Name = "Test Name" };

            // Act
            var serialized = await serializer.SerializeAsync(testData);
            var deserialized = await serializer.DeserializeAsync(serialized);

            // Assert
            Assert.That(deserialized, Is.Not.Null);
            Assert.That(deserialized!.Id, Is.EqualTo(123));
            Assert.That(deserialized.Name, Is.EqualTo("Test Name"));
        }

        // Define a test data class
        public class TestData
        {
            public int Id { get; set; }

            public string? Name { get; set; }

            public override bool Equals(object? obj)
            {
                return obj is TestData data &&
                       Id == data.Id &&
                       Name == data.Name;
            }

            public override int GetHashCode()
            {
                return HashCode.Combine(Id, Name);
            }
        }

        // Test SyncSerializationStrategyBase implementation
        private class JsonSyncSerializer : SyncSerializationStrategyBase<TestData>
        {
            public override TestData? Deserialize(byte[]? bytes)
            {
                if (bytes == null) return null;
                var json = System.Text.Encoding.UTF8.GetString(bytes);
                return System.Text.Json.JsonSerializer.Deserialize<TestData>(json);
            }

            public override byte[]? Serialize(TestData? obj)
            {
                if (obj == null) return null;
                var json = System.Text.Json.JsonSerializer.Serialize(obj);
                return System.Text.Encoding.UTF8.GetBytes(json);
            }
        }

        // Test AsyncSerializationStrategyBase implementation
        private class JsonAsyncSerializer : AsyncSerializationStrategyBase<TestData>
        {
            public override async ValueTask<TestData?> DeserializeAsync(byte[]? bytes, CancellationToken cancellationToken = default)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (bytes == null) return null;

                using var stream = new MemoryStream(bytes);
                return await System.Text.Json.JsonSerializer.DeserializeAsync<TestData>(
                    stream,
                    cancellationToken: cancellationToken);
            }

            public override async ValueTask<byte[]?> SerializeAsync(TestData? obj, CancellationToken cancellationToken = default)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (obj == null) return null;

                using var stream = new MemoryStream();
                await System.Text.Json.JsonSerializer.SerializeAsync(
                    stream,
                    obj,
                    cancellationToken: cancellationToken);
                return stream.ToArray();
            }
        }
    }
}