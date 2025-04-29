using Microsoft.Extensions.Logging;
using NSubstitute;
using NUnit.Framework;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace ModernDiskQueue.Tests
{
    [TestFixture]
    public class LongTermDequeueTestsAsync
    {
        private IPersistentQueue? _q;

        private IPersistentQueueFactory  _factory = Substitute.For<IPersistentQueueFactory>();
        [SetUp]
        public async Task Setup()
        {
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.SetMinimumLevel(LogLevel.Information);
                builder.AddConsole();
            });
            _factory = new PersistentQueueFactory(loggerFactory);
            _q = await _factory.WaitForAsync("./LongTermDequeueTests", TimeSpan.FromSeconds(10));
        }

        [TearDown]
        public async Task Teardown()
        {
            if (_q != null)
            {
                await _q.DisposeAsync();
            }
        }

        [Test]
        public async Task can_enqueue_during_a_long_dequeue()
        {
            if (_q == null) throw new InvalidOperationException("Queue is null");

            var s1 = await _q.OpenSessionAsync();

            await using (var s2 = await _q.OpenSessionAsync())
            {
                if (s2 == null) throw new InvalidOperationException("Session 2 is null");
                await s2.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                await s2.FlushAsync();
            }

            var x = await s1.DequeueAsync();
            await s1.FlushAsync();
            await s1.DisposeAsync();

            Assert.That(x!.SequenceEqual(new byte[] { 1, 2, 3, 4 }));
        }
    }
}