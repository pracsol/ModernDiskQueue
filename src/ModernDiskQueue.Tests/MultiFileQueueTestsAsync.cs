using NUnit.Framework;
using System.IO;
using System.Threading.Tasks;
// ReSharper disable AssignNullToNotNullAttribute

namespace ModernDiskQueue.Tests
{
    [TestFixture, SingleThreaded]
    public class MultiFileQueueTestsAsync : PersistentQueueTestsBase
    {
        protected override string QueuePath => "./MultiFileQueue";

        [Test]
        public async Task Can_limit_amount_of_items_in_queue_file()
        {
            using (IPersistentQueue queue = await PersistentQueue.CreateAsync(QueuePath, 10))
            {
                Assert.That(10, Is.EqualTo(queue.MaxFileSize));
            }
        }

        [Test]
        public async Task Entering_more_than_count_of_items_will_work()
        {
            using (var queue = await PersistentQueue.CreateAsync(QueuePath, 10))
            {
                for (byte i = 0; i < 11; i++)
                {
                    using (var session = await queue.OpenSessionAsync())
                    {
                        await session.EnqueueAsync(new[] { i });
                        await session.FlushAsync();
                    }
                }
                Assert.That(11, Is.EqualTo(await queue.GetEstimatedCountOfItemsInQueueAsync()));
            }
        }

        [Test]
        public async Task When_creating_more_items_than_allowed_in_first_file_will_create_additional_file()
        {
            using (var queue = await PersistentQueue.CreateAsync(QueuePath, 10))
            {
                for (byte i = 0; i < 11; i++)
                {
                    using (var session = await queue.OpenSessionAsync())
                    {
                        await session.EnqueueAsync(new[] { i });
                        await session.FlushAsync();
                    }
                }
                Assert.That(1, Is.EqualTo(queue.Internals.CurrentFileNumber));
            }
        }

        [Test]
        public async Task Can_resume_writing_to_second_file_when_restart_queue()
        {
            using (var queue = await PersistentQueue.CreateAsync(QueuePath, 10))
            {
                for (byte i = 0; i < 11; i++)
                {
                    using (var session = await queue.OpenSessionAsync())
                    {
                        await session.EnqueueAsync([i]);
                        await session.FlushAsync();
                    }
                }
                Assert.That(1, Is.EqualTo(queue.Internals.CurrentFileNumber));
            }
            using (var queue = await PersistentQueue.CreateAsync(QueuePath, 10))
            {
                for (byte i = 0; i < 2; i++)
                {
                    using (var session = await queue.OpenSessionAsync())
                    {
                        await session.EnqueueAsync([i]);
                        await session.FlushAsync();
                    }
                }
                Assert.That(1, Is.EqualTo(queue.Internals.CurrentFileNumber));
            }
        }

        [Test]
        public async Task Can_dequeue_from_all_files()
        {
            using (var queue = await PersistentQueue.CreateAsync(QueuePath, 10))
            {
                for (byte i = 0; i < 12; i++)
                {
                    using (var session = await queue.OpenSessionAsync())
                    {
                        await session.EnqueueAsync(new[] { i });
                        await session.FlushAsync();
                    }
                }
                Assert.That(1, Is.EqualTo(queue.Internals.CurrentFileNumber));
            }

            using (var queue = await PersistentQueue.CreateAsync(QueuePath, 10))
            {
                for (byte i = 0; i < 12; i++)
                {
                    using (var session = await queue.OpenSessionAsync())
                    {
                        var value = await session.DequeueAsync();
                        Assert.That(value, Is.Not.Null);
                        Assert.That(i, Is.EqualTo(value?[0]));
                        await session.FlushAsync();
                    }
                }
            }
        }

        [Test]
        public async Task Can_dequeue_from_all_files_after_restart()
        {
            using (var queue = await PersistentQueue.CreateAsync(QueuePath, 10))
            {
                for (byte i = 0; i < 12; i++)
                {
                    using (var session = await queue.OpenSessionAsync())
                    {
                        await session.EnqueueAsync(new[] { i });
                        await session.FlushAsync();
                    }
                }
                Assert.That(1, Is.EqualTo(queue.Internals.CurrentFileNumber));
            }

            using (var queue = await PersistentQueue.CreateAsync(QueuePath, 10))
            {
                for (byte i = 0; i < 3; i++)
                {
                    using (var session = await queue.OpenSessionAsync())
                    {
                        await session.EnqueueAsync(new[] { i });
                        await session.FlushAsync();
                    }
                }
                Assert.That(1, Is.EqualTo(queue.Internals.CurrentFileNumber));
            }


            using (var queue = await PersistentQueue.CreateAsync(QueuePath, 10))
            {
                using (var session = await queue.OpenSessionAsync())
                {
                    for (byte i = 0; i < 12; i++)
                    {
                        var value = await session.DequeueAsync();
                        Assert.That(value, Is.Not.Null);
                        Assert.That(i, Is.EqualTo(value?[0]));
                        await session.FlushAsync();
                    }

                    for (byte i = 0; i < 3; i++)
                    {
                        var value = await session.DequeueAsync();
                        Assert.That(value, Is.Not.Null);
                        Assert.That(i, Is.EqualTo(value?[0]));
                        await session.FlushAsync();
                    }
                }
            }
        }

        [Test]
        public async Task After_reading_all_items_from_file_that_is_not_the_active_file_should_delete_file()
        {
            using (var queue = await PersistentQueue.CreateAsync(QueuePath, 10))
            {
                for (byte i = 0; i < 12; i++) // 12 individual bytes, and a 10 byte file limit
                {
                    using (var session = await queue.OpenSessionAsync())
                    {
                        await session.EnqueueAsync([i]);
                        await session.FlushAsync();
                    }
                }
                Assert.That(1, Is.EqualTo(queue.Internals.CurrentFileNumber));
            }

            using (var queue = await PersistentQueue.CreateAsync(QueuePath, 10))
            {
                for (byte i = 0; i < 12; i++)
                {
                    using (var session = await queue.OpenSessionAsync())
                    {
                        var value = await session.DequeueAsync();
                        Assert.That(value, Is.Not.Null);
                        Assert.That(i, Is.EqualTo(value?[0]));
                        await session.FlushAsync();
                    }
                }
            }

            Assert.That(
                File.Exists(System.IO.Path.Combine(QueuePath, "data.0")), Is.False
                );
        }
    }
}