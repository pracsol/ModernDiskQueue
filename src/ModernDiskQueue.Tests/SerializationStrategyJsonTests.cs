// -----------------------------------------------------------------------
// <copyright file="SerializationStrategyJsonTests.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text.Json;
    using System.Text.Json.Serialization;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using ModernDiskQueue.Implementation;
    using NSubstitute;
    using NUnit.Framework;

    [TestFixture]
    public class SerializationStrategyJsonTests
    {
        // Note: having the queue files shared between the tests checks that we
        // are correctly closing down the queue (i.e. the `Dispose()` call works)
        // If not, one of the files will fail complaining that the lock is still held.
        private const string QueueName = "./SerializationStrategyJsonTests";

        private IPersistentQueueFactory _factory = Substitute.For<IPersistentQueueFactory>();

        [SetUp]
        public void Setup()
        {
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.SetMinimumLevel(LogLevel.Information);
                builder.AddConsole();
            });
            _factory = new PersistentQueueFactory(loggerFactory);
        }

        [Test]
        public async Task StrategyJson_DynamicType_DeserializeShouldFail()
        {
            await using var queue = await _factory.CreateAsync<TestClass>(QueueName + "dynamicproperty_json");
            var strategy = new SerializationStrategyJson<TestClass>();
            await using var session = await queue.OpenSessionAsync(strategy);

            TestClass testObject = new(7, "TestString", -5);
            testObject.ArbitraryObjectType = new object();
            testObject.ArbitraryDynamicType = "test";
            await session.EnqueueAsync(testObject);
            await session.FlushAsync();
            TestClass testObject2 = await session.DequeueAsync();
            await session.FlushAsync();

            Assert.That(testObject2, Is.Not.Null);

            // Note that this assertion will pass, since it's not checking the dynamic type?
            Assert.That(testObject, Is.EqualTo(testObject2));

            // This assertion will fail because of the behavior of the Json serializer.
            Assert.That(testObject.ArbitraryDynamicType, Is.Not.EqualTo(testObject2.ArbitraryDynamicType), "Dynamic type values were equal.");
        }

        [Test]
        public async Task StrategyJson_PrivateField_DeserializeShouldFail()
        {
            await using var queue = await _factory.CreateAsync<TestClass>(QueueName + "privatefield_json");
            var jsonStrategy = new SerializationStrategyJson<TestClass>();
            await using var session = await queue.OpenSessionAsync(jsonStrategy);

            TestClass testObject = new(7, "TestString", -5);
            testObject.SetInternalField(5);
            await session.EnqueueAsync(testObject);
            await session.FlushAsync();
            TestClass testObject2 = await session.DequeueAsync();
            int internalField = testObject2.GetInternalField();
            await session.FlushAsync();

            Assert.That(testObject2, Is.Not.Null);
            Assert.That(testObject, Is.EqualTo(testObject2));
            Assert.That(internalField, Is.Not.EqualTo(5));
        }

        [Test]
        public async Task StrategyJson_NonDefaultOptionsProvided_OptionsAreUsed()
        {
            await using var queue = await _factory.CreateAsync<TestClass>(QueueName + "options_json");
            var options = new JsonSerializerOptions
            {
                MaxDepth = 1,
                WriteIndented = true,
            };
            var jsonStrategy = new SerializationStrategyJson<TestClass>(options);
            await using var session1 = await queue.OpenSessionAsync(jsonStrategy);

            TestClass testObject = new(7, "TestString", -5);
            Assert.ThrowsAsync<JsonException>(async () => await session1.EnqueueAsync(testObject), "MaxDepth option was not respected.");
            await session1.DisposeAsync();

            // Use the default options with maxdepth of 64
            await using var session2 = await queue.OpenSessionAsync();
            Assert.DoesNotThrowAsync(async () => await session2.EnqueueAsync(testObject), "Default MaxDepth was used.");
            await session2.FlushAsync();
        }
    }
}