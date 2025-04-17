using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

// ReSharper disable PossibleNullReferenceException

namespace ModernDiskQueue.Tests
{
    [TestFixture]
    public class MultipleProcessAccessTestsAsync : PersistentQueueTestsBase
    {
        protected override string QueuePath => "./MultipleProcessAccessTests";

        [Test,
         Description("Multiple PersistentQueue instances are " +
                     "pretty much the same as multiple processes to " +
                     "the DiskQueue library")]
        public void Can_access_from_multiple_queues_if_used_carefully()
        {
            var producerDone = new ManualResetEvent(false);
            var consumerDone = new ManualResetEvent(false);
            var received = new List<byte[]>();
            int numberOfItems = 10;

            var t1 = new Thread(() =>
            {
                for (int i = 0; i < numberOfItems; i++)
                {
                    var task = AddToQueueAsync(new byte[] {1, 2, 3 });
                    task.Wait();
                }
                producerDone.Set();
            });
            var t2 = new Thread(() =>
            {
                while (received.Count < numberOfItems)
                {
                    var task = ReadQueueAsync();
                    task.Wait();
                    var data = task.Result;
                    if (data != null)
                    {
                        lock(received)
                        {
                            received.Add(data);
                        }
                    }
                }
                consumerDone.Set();
            });

            t1.Start();
            t2.Start();

            // Wait for producer to finish
            var producerFinished = producerDone.WaitOne(TimeSpan.FromSeconds(30));

            // Wait for consumer to finish
            var consumerFinished = consumerDone.WaitOne(TimeSpan.FromSeconds(25));

            if (!producerFinished)
            {
                Assert.Fail("Producer did not finish in time");
            }
            if (!consumerFinished)
            {
                Assert.Fail("Consumer did not finish in time");
            }

            Console.WriteLine($"Added {numberOfItems} items to queue.");
            Console.WriteLine($"Recieved {received.Count} items back.");
            Assert.That(received.Count, Is.EqualTo(numberOfItems), "Count of received items did not match number of items added.");
        }

        [Test]
        public async Task Can_access_from_multiple_queues_if_used_carefully_with_generic_container_and_serialisation()
        {
            var producerDone = new ManualResetEvent(false);
            var consumerDone = new ManualResetEvent(false);
            var received = new List<string>();
            int numberOfItems = 10;

            var t1 = new Thread(() =>
            {
                for (int i = 0; i < numberOfItems; i++)
                {
                    var task = AddToQueueStringAsync("Hello");
                    task.Wait();
                }
                producerDone.Set();
            });
            var t2 = new Thread(() => 
            {
                while (received.Count < numberOfItems)
                {
                    var task = ReadQueueStringAsync();
                    var data = task.Result;
                    if (data != null)
                    {
                        lock(received)
                        {
                            received.Add(data);
                        }
                    }
                }
                consumerDone.Set();
            });

            t1.Start();
            t2.Start();

            // Wait for producer to finish
            var producerFinished = producerDone.WaitOne(TimeSpan.FromSeconds(30));

            // Wait for consumer to finish
            var consumerFinished = consumerDone.WaitOne(TimeSpan.FromSeconds(25));

            if (!producerFinished)
            {
                Assert.Fail("Producer did not finish in time");
            }
            if (!consumerFinished)
            {
                Assert.Fail("Consumer did not finish in time");
            }

            Assert.That(received.Count, Is.EqualTo(numberOfItems), "received items");
        }

        private async Task AddToQueueStringAsync(string data)
        {
            Thread.Sleep(152);
            using var queue = await PersistentQueue.WaitForAsync<string>(QueuePath, TimeSpan.FromSeconds(30));
            using var session = await queue.OpenSessionAsync();

            await session.EnqueueAsync(data);
            await session.FlushAsync();
        }

        private async Task<string?> ReadQueueStringAsync()
        {
            Thread.Sleep(121);
            using var queue = await PersistentQueue.WaitForAsync<string>(QueuePath, TimeSpan.FromSeconds(30));
            using var session = await queue.OpenSessionAsync();
            var data = await session.DequeueAsync();
            await session.FlushAsync();
            return data;
        }

        private async Task AddToQueueAsync(byte[] data)
        {
            Thread.Sleep(152);
            using (var queue = await PersistentQueue.WaitForAsync(QueuePath, TimeSpan.FromSeconds(30)))
            {
                using (var session = await queue.OpenSessionAsync())
                {
                    await session.EnqueueAsync(data);
                    await session.FlushAsync();
                }
            }
        }

        private async Task<byte[]?> ReadQueueAsync()
        {
            Thread.Sleep(121);
            using (var queue = await PersistentQueue.WaitForAsync(QueuePath, TimeSpan.FromSeconds(30)))
            {
                using (var session = await queue.OpenSessionAsync())
                {
                    var data = await session.DequeueAsync();
                    await session.FlushAsync();
                    return data;
                }
            }
        }
    }
}