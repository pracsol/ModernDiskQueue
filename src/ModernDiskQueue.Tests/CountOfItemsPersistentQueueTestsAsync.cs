using NUnit.Framework;
using System.Threading;
using System.Threading.Tasks;
// ReSharper disable PossibleNullReferenceException

namespace ModernDiskQueue.Tests
{
    [TestFixture, SingleThreaded]
    public class CountOfItemsPersistentQueueTestsAsync : PersistentQueueTestsBase
    {
        protected override string QueuePath => "./CountOfItemsTests";

        [Test]
        public async Task Can_get_count_from_queue()
        {
            using (var queue = await PersistentQueue.CreateAsync(QueuePath))
            {
                Assert.That(0, Is.EqualTo(await queue.GetEstimatedCountOfItemsInQueueAsync()));
            }
        }

        [Test]
        public async Task Can_enter_items_and_get_count_of_items()
        {
            using (var queue = await PersistentQueue.CreateAsync(QueuePath))
            {
                for (byte i = 0; i < 5; i++)
                {
                    using (var session = await queue.OpenSessionAsync())
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
            using (var queue = await PersistentQueue.CreateAsync(QueuePath))
            {
                for (byte i = 0; i < 5; i++)
                {
                    using (var session = await queue.OpenSessionAsync())
                    {
                        await session.EnqueueAsync(new[] { i });
                        await session.FlushAsync();
                    }
                }
            }

            using (var queue = await PersistentQueue.CreateAsync(QueuePath))
            {
                Assert.That(5, Is.EqualTo(await queue.GetEstimatedCountOfItemsInQueueAsync()));
            }
        }
    }
}