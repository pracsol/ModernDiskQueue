// <copyright file="GenericQueueTestsAsync.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>

// ReSharper disable AssignNullToNotNullAttribute
namespace ModernDiskQueue.Tests
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using ModernDiskQueue.Implementation;
    using NSubstitute;
    using NUnit.Framework;

    [TestFixture]
    [SingleThreaded]
    public class GenericQueueTestsAsync
    {
        // Note: having the queue files shared between the tests checks that we
        // are correctly closing down the queue (i.e. the `Dispose()` call works)
        // If not, one of the files will fail complaining that the lock is still held.
        private const string QueueName = "./GenericQueueTestsAsync";

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
        public async Task Round_trip_value_type()
        {
            await using IPersistentQueue<int> queue = await _factory.CreateAsync<int>(QueueName + "int");
            await using var session = await queue.OpenSessionAsync();

            await session.EnqueueAsync(7);
            await session.FlushAsync();
            var testNumber = await session.DequeueAsync();
            await session.FlushAsync();
            Assert.That(7, Is.EqualTo(testNumber));
        }

        [TestCase("Test")]
        [TestCase("")]
        [TestCase(" Leading Spaces")]
        [TestCase("Trailing Spaces   ")]
        [TestCase("A string longer than the others but still quite short")]
        [TestCase("other \r\n\t\b characters")]
        public async Task Round_trip_string_type(string valueToTest)
        {
            // Use different queue for each test case so that we don't get errors when running tests concurrently.
            var hash = valueToTest.GetHashCode().ToString("X8");
            await using var queue = await _factory.CreateAsync<string>($"./GenericQueueTests3{hash}");
            await using var session = await queue.OpenSessionAsync();

            while (await session.DequeueAsync() != null)
            {
                Console.WriteLine("Removing old data");
            }

            await session.FlushAsync();

            await session.EnqueueAsync(valueToTest);
            await session.FlushAsync();
            var returnValue = await session.DequeueAsync();
            await session.FlushAsync();
            Assert.That(valueToTest, Is.EqualTo(returnValue));
        }

        [Test]
        public async Task Round_trip_string_with_xml_strategy()
        {
            var testString = "Test string with special characters: \r\n\t\b!@#$%^&*()";
            await using var queue = await _factory.CreateAsync<string>(QueueName + "string_xml");
            var xmlStrategy = new SerializationStrategyXml<string>();
            await using var session = await queue.OpenSessionAsync(xmlStrategy);

            await session.EnqueueAsync(testString);
            await session.FlushAsync();
            var returnValue = await session.DequeueAsync();
            await session.FlushAsync();

            Assert.That(returnValue, Is.EqualTo(testString));
            Assert.That(session.SerializationStrategy, Is.SameAs(xmlStrategy));
        }

        [Test]
        public async Task Round_trip_string_with_json_strategy()
        {
            var testString = "Test string with special characters: \r\n\t\b!@#$%^&*()";
            await using var queue = await _factory.CreateAsync<string>(QueueName + "string_json");
            var jsonStrategy = new SerializationStrategyJson<string>();
            await using var session = await queue.OpenSessionAsync(jsonStrategy);

            await session.EnqueueAsync(testString);
            await session.FlushAsync();
            var returnValue = await session.DequeueAsync();
            await session.FlushAsync();

            Assert.That(returnValue, Is.EqualTo(testString));
            Assert.That(session.SerializationStrategy, Is.SameAs(jsonStrategy));
        }

        [Test]
        public async Task Round_trip_complex_type_with_xml_strategy()
        {
            await using var queue = await _factory.CreateAsync<TestClass>(QueueName + "complex_xml");
            var xmlStrategy = new SerializationStrategyXml<TestClass>();
            await using var session = await queue.OpenSessionAsync(xmlStrategy);

            var testObject = new TestClass(7, "TestString", null);
            await session.EnqueueAsync(testObject);
            await session.FlushAsync();
            var testObject2 = await session.DequeueAsync();
            await session.FlushAsync();

            Assert.That(testObject2, Is.Not.Null);
            Assert.That(testObject, Is.EqualTo(testObject2));
            Assert.That(session.SerializationStrategy, Is.SameAs(xmlStrategy));

            testObject = new TestClass(7, "TestString", -5);
            await session.EnqueueAsync(testObject);
            await session.FlushAsync();
            testObject2 = await session.DequeueAsync();
            await session.FlushAsync();

            Assert.That(testObject2, Is.Not.Null);
            Assert.That(testObject, Is.EqualTo(testObject2));
            Assert.That(testObject.ArbitraryDynamicType, Is.EqualTo(testObject2.ArbitraryDynamicType));
        }

        [Test]
        public async Task Round_trip_complex_type_with_json_strategy()
        {
            await using var queue = await _factory.CreateAsync<TestClass>(QueueName + "complex_json");
            var jsonStrategy = new SerializationStrategyJson<TestClass>();
            await using var session = await queue.OpenSessionAsync(jsonStrategy);

            var testObject = new TestClass(7, "TestString", null);
            await session.EnqueueAsync(testObject);
            await session.FlushAsync();
            var testObject2 = await session.DequeueAsync();
            await session.FlushAsync();

            Assert.That(testObject2, Is.Not.Null);
            Assert.That(testObject, Is.EqualTo(testObject2));
            Assert.That(session.SerializationStrategy, Is.SameAs(jsonStrategy));

            testObject = new TestClass(7, "TestString", -5);
            testObject.ArbitraryObjectType = new object();
            testObject.ArbitraryDynamicType = "test";
            await session.EnqueueAsync(testObject);
            await session.FlushAsync();
            testObject2 = await session.DequeueAsync();
            await session.FlushAsync();

            Assert.That(testObject2, Is.Not.Null);
            Assert.That(testObject, Is.EqualTo(testObject2));
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
        public async Task StrategyXml_DynamicType_DeserializeShouldSucceed()
        {
            await using var queue = await _factory.CreateAsync<TestClass>(QueueName + "dynamicproperty_xml");
            var strategy = new SerializationStrategyXml<TestClass>();
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
            Assert.That(testObject.ArbitraryDynamicType, Is.EqualTo(testObject2.ArbitraryDynamicType), "Dynamic type values were not equal.");
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
        public async Task StrategyXml_PrivateField_DeserializeShouldSucceed()
        {
            await using var queue = await _factory.CreateAsync<TestClass>(QueueName + "privatefield_xml");
            var xmlStrategy = new SerializationStrategyXml<TestClass>();
            await using var session = await queue.OpenSessionAsync(xmlStrategy);

            TestClass testObject = new(7, "TestString", -5);
            testObject.SetInternalField(5);
            await session.EnqueueAsync(testObject);
            await session.FlushAsync();
            TestClass testObject2 = await session.DequeueAsync();
            int internalField = testObject2.GetInternalField();
            await session.FlushAsync();

            Assert.That(testObject2, Is.Not.Null);
            Assert.That(testObject, Is.EqualTo(testObject2));
            Assert.That(internalField, Is.EqualTo(5));
        }

        [Test]
        public async Task Round_trip_complex_type()
        {
            await using var queue = await _factory.CreateAsync<TestClass>(QueueName + "TC");
            await using var session = await queue.OpenSessionAsync();

            var testObject = new TestClass(7, "TestString", null);
            await session.EnqueueAsync(testObject);
            await session.FlushAsync();
            var testObject2 = await session.DequeueAsync();
            await session.FlushAsync();
            Assert.That(testObject2, Is.Not.Null);
            Assert.That(testObject, Is.EqualTo(testObject2));

            testObject = new TestClass(7, "TestString", -5);
            await session.EnqueueAsync(testObject);
            await session.FlushAsync();
            testObject2 = await session.DequeueAsync();
            await session.FlushAsync();
            Assert.That(testObject2, Is.Not.Null);
            Assert.That(testObject, Is.EqualTo(testObject2));
        }

        [Test]
        public async Task Round_trip_DateTimeOffset()
        {
            await using var queue = await _factory.CreateAsync<DateTimeOffset>(QueueName + "TC2");
            await using var session = await queue.OpenSessionAsync();

            var testObject = DateTimeOffset.Now;
            await session.EnqueueAsync(testObject);
            await session.FlushAsync();
            var testObject2 = await session.DequeueAsync();
            await session.FlushAsync();
            Assert.That(testObject2, Is.Not.Null);
            Assert.That(testObject, Is.EqualTo(testObject2));
        }

        [Test]
        public async Task SessionSerializer_NotSpecified_ShouldBeXml()
        {
            await using var queue = await _factory.CreateAsync<DateTimeOffset>(QueueName);
            await using var session = await queue.OpenSessionAsync();
            Assert.That(session.SerializationStrategy, Is.InstanceOf<SerializationStrategyXml<DateTimeOffset>>());
        }

        [Test]
        public async Task SessionSerializer_PassedInSessionConstructor_ShouldBeValueSpecified()
        {
            await using var queue = await _factory.CreateAsync<DateTimeOffset>(QueueName);
            await using var session = await queue.OpenSessionAsync(new SerializationStrategyJson<DateTimeOffset>());
            Assert.That(session.SerializationStrategy, Is.InstanceOf<SerializationStrategyJson<DateTimeOffset>>());
        }

        [Test]
        public async Task SessionSerializer_SetInQueueConstructor_ShouldBeValueSpecified()
        {
            await using var queue = await _factory.CreateAsync<DateTimeOffset>(QueueName, new SerializationStrategyJson<DateTimeOffset>());
            await using var session = await queue.OpenSessionAsync();
            Assert.That(session.SerializationStrategy, Is.InstanceOf<SerializationStrategyJson<DateTimeOffset>>());
        }

        [Test]
        public async Task SessionSerializer_SetInQueueConstructorUsingEnum_ShouldBeValueSpecified()
        {
            await using var queue = await _factory.CreateAsync<DateTimeOffset>(QueueName, SerializationStrategy.Json);
            await using var session = await queue.OpenSessionAsync();
            Assert.That(session.SerializationStrategy, Is.InstanceOf<SerializationStrategyJson<DateTimeOffset>>());
            await session.DisposeAsync();
            await queue.DisposeAsync();

            await using var queue2 = await _factory.CreateAsync<DateTimeOffset>(QueueName, SerializationStrategy.Xml);
            await using var session2 = await queue2.OpenSessionAsync();
            Assert.That(session2.SerializationStrategy, Is.InstanceOf<SerializationStrategyXml<DateTimeOffset>>());
            await session2.DisposeAsync();
            await queue2.DisposeAsync();
        }

        [Test]
        public async Task SessionSerializer_OverrideQueueDefault_ShouldBeValueSpecified()
        {
            await using var queue = await _factory.CreateAsync<DateTimeOffset>(QueueName, SerializationStrategy.Json);
            await using var session = await queue.OpenSessionAsync();
            Assert.That(session.SerializationStrategy, Is.InstanceOf<SerializationStrategyJson<DateTimeOffset>>(), "Queue default strategy was not employed when expected.");
            await session.DisposeAsync();

            var newXmlSession = await queue.OpenSessionAsync(new SerializationStrategyXml<DateTimeOffset>());
            Assert.That(newXmlSession.SerializationStrategy, Is.InstanceOf<SerializationStrategyXml<DateTimeOffset>>(), "Session constructor strategy did not override the queue default.");
            await newXmlSession.DisposeAsync();

            var newXmlSession2 = await queue.OpenSessionAsync();
            newXmlSession2.SerializationStrategy = new SerializationStrategyXml<DateTimeOffset>();
            Assert.That(newXmlSession2.SerializationStrategy, Is.InstanceOf<SerializationStrategyXml<DateTimeOffset>>(), "Session property strategy did not override the queue default.");
            await newXmlSession2.DisposeAsync();

            var newXmlSession3 = await queue.OpenSessionAsync(SerializationStrategy.Xml);
            Assert.That(newXmlSession3.SerializationStrategy, Is.InstanceOf<SerializationStrategyXml<DateTimeOffset>>(), "Session constructor with enum did not override the queue default.");
            await newXmlSession3.DisposeAsync();
        }
    }
}