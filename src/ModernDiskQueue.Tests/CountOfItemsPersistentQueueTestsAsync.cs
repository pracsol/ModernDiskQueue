using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NSubstitute;
using System.Threading;
using System.Threading.Tasks;
// ReSharper disable PossibleNullReferenceException

namespace ModernDiskQueue.Tests
{
    [TestFixture, SingleThreaded]
    public class CountOfItemsPersistentQueueTestsAsync : PersistentQueueTestsBase
    {
        protected override string QueuePath => "./CountOfItemsTests";
        private IPersistentQueueFactory  _factory = Substitute.For<IPersistentQueueFactory>();
        [SetUp]
        public new void Setup()
        {
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.SetMinimumLevel(LogLevel.Information);
                builder.AddConsole();
            });
            _factory = new PersistentQueueFactory(loggerFactory);
            base.Setup();
        }

        [Test]
        public async Task Can_get_count_from_queue()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                Assert.That(0, Is.EqualTo(await queue.GetEstimatedCountOfItemsInQueueAsync()));
            }
        }

        [Test]
        public async Task Can_enter_items_and_get_count_of_items()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                for (byte i = 0; i < 5; i++)
                {
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        await session.EnqueueAsync([i]);
                        await session.FlushAsync();
                    }
                }
                Assert.That(5, Is.EqualTo(await queue.GetEstimatedCountOfItemsInQueueAsync()));
            }
        }

        [Test]
        public async Task Can_get_count_of_items_after_queue_restart()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                for (byte i = 0; i < 5; i++)
                {
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        await session.EnqueueAsync(new[] { i });
                        await session.FlushAsync();
                    }
                }
            }

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                Assert.That(5, Is.EqualTo(await queue.GetEstimatedCountOfItemsInQueueAsync()));
            }
        }
    }
}