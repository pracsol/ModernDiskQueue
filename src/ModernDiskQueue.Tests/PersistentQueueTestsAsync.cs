using ModernDiskQueue.Implementation;
using NUnit.Framework;
using System;
using System.Threading.Tasks;

namespace ModernDiskQueue.Tests
{
    [TestFixture, SingleThreaded]
    public class PersistentQueueTestsAsync : PersistentQueueTestsBase
    {
        protected override string Path => "./AsyncPersistentQueueTests";

        [Test]
        public async Task Can_create_new_queue_async()
        {
            var queue = await PersistentQueue.WaitForAsync(Path, TimeSpan.FromSeconds(5));
            using (queue)
            {
                Assert.That(queue, Is.Not.Null);
            }
        }

        [Test]
        public async Task Can_hard_delete_async()
        {
            var queue = await PersistentQueue.WaitForAsync(Path, TimeSpan.FromSeconds(5));
            await queue.HardDeleteAsync(true);

            // Verify the queue was reset by checking if we can create a new one
            var newQueue = await PersistentQueue.WaitForAsync(Path, TimeSpan.FromSeconds(5));
            Assert.That(newQueue, Is.Not.Null);
            using (newQueue) { }
        }

        [Test]
        public async Task Dequeueing_from_empty_queue_will_return_null_async()
        {
            var queue = await PersistentQueue.WaitForAsync(Path, TimeSpan.FromSeconds(5));
            using (queue)
            {
                var session = await queue.OpenSessionAsync();
                var dequeued = await session.DequeueAsync();
                Assert.That(dequeued, Is.Null);
                await session.FlushAsync();
            }
        }

        [Test]
        public async Task Can_enqueue_data_in_queue_async()
        {
            var queue = await PersistentQueue.WaitForAsync(Path, TimeSpan.FromSeconds(5));
            using (queue)
            {
                var session = await queue.OpenSessionAsync();
                await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                await session.FlushAsync();
            }
        }

        [Test]
        public async Task Can_dequeue_data_from_queue_async()
        {
            var queue = await PersistentQueue.WaitForAsync(Path, TimeSpan.FromSeconds(5));
            using (queue)
            {
                var session = await queue.OpenSessionAsync();
                await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                await session.FlushAsync();
                var dequeued = await session.DequeueAsync();
                Assert.That(dequeued, Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
            }
        }

        [Test]
        public async Task Queueing_and_dequeueing_empty_data_is_handled_async()
        {
            var queue = await PersistentQueue.WaitForAsync(Path, TimeSpan.FromSeconds(5));
            using (queue)
            {
                var session = await queue.OpenSessionAsync();
                await session.EnqueueAsync(Array.Empty<byte>());
                await session.FlushAsync();
                var dequeued = await session.DequeueAsync();
                Assert.That(dequeued, Is.EqualTo(Array.Empty<byte>()));
            }
        }

        [Test]
        public async Task Can_enqueue_and_dequeue_data_after_restarting_queue_async()
        {
            // First session: enqueue data
            {
                var queue = await PersistentQueue.WaitForAsync(Path, TimeSpan.FromSeconds(5));
                using (queue)
                {
                    var session = await queue.OpenSessionAsync();
                    await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                    await session.FlushAsync();
                }
            }

            // Second session: dequeue data
            {
                var queue = await PersistentQueue.WaitForAsync(Path, TimeSpan.FromSeconds(5));
                using (queue)
                {
                    var session = await queue.OpenSessionAsync();
                    var dequeued = await session.DequeueAsync();
                    Assert.That(dequeued, Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                    await session.FlushAsync();
                }
            }
        }

        [Test]
        public async Task After_dequeue_from_queue_item_no_longer_on_queue_async()
        {
            // First session: enqueue data
            {
                var queue = await PersistentQueue.WaitForAsync(Path, TimeSpan.FromSeconds(5));
                using (queue)
                {
                    var session = await queue.OpenSessionAsync();
                    await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                    await session.FlushAsync();
                }
            }

            // Second session: dequeue and verify queue is empty
            {
                var queue = await PersistentQueue.WaitForAsync(Path, TimeSpan.FromSeconds(5));
                using (queue)
                {
                    var session = await queue.OpenSessionAsync();
                    var dequeued = await session.DequeueAsync();
                    Assert.That(dequeued, Is.EqualTo(new byte[] { 1, 2, 3, 4 }));

                    var shouldBeNull = await session.DequeueAsync();
                    Assert.That(shouldBeNull, Is.Null);

                    await session.FlushAsync();
                }
            }
        }

        [Test]
        public async Task Not_flushing_the_session_will_revert_dequeued_items_async()
        {
            // First session: enqueue data
            {
                var queue = await PersistentQueue.WaitForAsync(Path, TimeSpan.FromSeconds(5));
                using (queue)
                {
                    var session = await queue.OpenSessionAsync();
                    await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                    await session.FlushAsync();
                }
            }

            // Second session: dequeue but don't flush
            {
                var queue = await PersistentQueue.WaitForAsync(Path, TimeSpan.FromSeconds(5));
                using (queue)
                {
                    var session = await queue.OpenSessionAsync();
                    var dequeued = await session.DequeueAsync();
                    Assert.That(dequeued, Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                    // Explicitly omit: await session.FlushAsync();
                }
            }

            // Third session: verify item is still there
            {
                var queue = await PersistentQueue.WaitForAsync(Path, TimeSpan.FromSeconds(5));
                using (queue)
                {
                    var session = await queue.OpenSessionAsync();
                    var dequeued = await session.DequeueAsync();
                    Assert.That(dequeued, Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                    await session.FlushAsync();
                }
            }
        }

        [Test]
        public async Task Not_flushing_the_session_will_revert_queued_items_async()
        {
            // First session: enqueue data but don't flush
            {
                var queue = await PersistentQueue.WaitForAsync(Path, TimeSpan.FromSeconds(5));
                using (queue)
                {
                    var session = await queue.OpenSessionAsync();
                    await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                    // Explicitly omit: await session.FlushAsync();
                }
            }

            // Second session: verify queue is empty
            {
                var queue = await PersistentQueue.WaitForAsync(Path, TimeSpan.FromSeconds(5));
                using (queue)
                {
                    var session = await queue.OpenSessionAsync();
                    var dequeued = await session.DequeueAsync();
                    Assert.That(dequeued, Is.Null);
                    await session.FlushAsync();
                }
            }
        }
    }
}
