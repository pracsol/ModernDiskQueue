using Microsoft.Extensions.Logging;
using NSubstitute;
using NUnit.Framework;
using System;
using System.Threading.Tasks;
// ReSharper disable AssignNullToNotNullAttribute

namespace ModernDiskQueue.Tests
{

    [TestFixture, SingleThreaded]
    public class GenericQueueTestsAsync
    {
        // Note: having the queue files shared between the tests checks that we 
        // are correctly closing down the queue (i.e. the `Dispose()` call works)
        // If not, one of the files will fail complaining that the lock is still held.
        private const string QueueName = "./GenericQueueTestsAsync";

        private IPersistentQueueFactory  _factory = Substitute.For<IPersistentQueueFactory>();
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
            await using PersistentQueue<int> queue = await _factory.CreateAsync<int>(QueueName+"int");
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

            while (await session.DequeueAsync() != null) { Console.WriteLine("Removing old data"); }
            await session.FlushAsync();

            await session.EnqueueAsync(valueToTest);
            await session.FlushAsync();
            var returnValue = await session.DequeueAsync();
            await session.FlushAsync();
            Assert.That(valueToTest, Is.EqualTo(returnValue));
        }

        [Test]
        public async Task Round_trip_complex_type()
        {
            await using var queue = await _factory.CreateAsync<TestClass>(QueueName+"TC");
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
            await using var queue   = await _factory.CreateAsync<DateTimeOffset>(QueueName+"TC2");
            await using var session = await queue.OpenSessionAsync();

            var testObject = DateTimeOffset.Now;
            await session.EnqueueAsync(testObject);
            await session.FlushAsync();
            var testObject2 = await session.DequeueAsync();
            await session.FlushAsync();
            Assert.That(testObject2, Is.Not.Null);
            Assert.That(testObject, Is.EqualTo(testObject2));
        }
    }
}