namespace ModernDiskQueue.Tests
{
    using NUnit.Framework;
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    [TestFixture, SingleThreaded]
    public class ThreadSafeAccessTestsAsync
    {
        [Test]
        public async Task can_enqueue_and_dequeue_on_separate_threads()
        {
            var producerDone = new ManualResetEvent(false);
            var consumerDone = new ManualResetEvent(false);
            int t1s, t2s;
            t1s = t2s = 0;
            const int target = 100;
            var rnd = new Random();
            Exception? lastException = null;

            IPersistentQueue subject = await PersistentQueue.CreateAsync("queue_ta");
            var t1 = new Thread(async () =>
            {
                try
                {
                    for (int i = 0; i < target; i++)
                    {
                        await using (var session = await subject.OpenSessionAsync())
                        {
                            Console.Write("(");
                            await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                            Interlocked.Increment(ref t1s);
                            await Task.Delay(rnd.Next(0, 100));
                            await session.FlushAsync();
                            Console.Write(")");
                        }
                        producerDone.Set();
                    }
                }
                catch (Exception ex)
                {
                    lastException = ex;
                    producerDone.Set();
                }
            });
            var t2 = new Thread(async ()=>
            {
                try
                {
                    for (int i = 0; i < target; i++)
                    {
                        await using (var session = await subject.OpenSessionAsync())
                        {
                            Console.Write("<");
                            await session.DequeueAsync();
                            Interlocked.Increment(ref t2s);
                            await Task.Delay(rnd.Next(0, 100));
                            await session.FlushAsync();
                            Console.Write(">");
                        }
                    }
                    consumerDone.Set();
                }
                catch (Exception ex)
                {
                    lastException = ex;
                    consumerDone.Set();
                }
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
            if (lastException != null)
            {
                Assert.Fail(lastException.Message);
            }
            Assert.That(t1s, Is.EqualTo(target));
            Assert.That(t2s, Is.EqualTo(target));

        }

        [Test]
        public async Task can_sequence_queues_on_separate_threads()
        {
            var producerDone = new ManualResetEvent(false);
            var consumerDone = new ManualResetEvent(false);
            int t1s, t2s;
            t1s = t2s = 0;
            const int target = 100;
            Exception? lastException = null;

            var t1 = new Thread(() =>
            {
                Task.Run(async () =>
                {
                    try
                    {

                        for (int i = 0; i < target; i++)
                        {
                            await using (var subject = await PersistentQueue.WaitForAsync("queue_tb", TimeSpan.FromSeconds(10)))
                            {
                                await using (var session = await subject.OpenSessionAsync())
                                {
                                    Console.Write("(");
                                    await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                                    Interlocked.Increment(ref t1s);
                                    await session.FlushAsync();
                                    Console.Write(")");
                                }
                                await Task.Delay(0);
                            }
                        }
                        producerDone.Set();
                    }
                    catch (Exception ex)
                    {
                        lastException = ex;
                        producerDone.Set();
                    }
                }).Wait();
            });
            var t2 = new Thread(()=>
            {
                Task.Run(async () =>
                {
                    try
                    {

                        for (int i = 0; i < target; i++)
                        {
                            await using (var subject = await PersistentQueue.WaitForAsync("queue_tb", TimeSpan.FromSeconds(10)))
                            {
                                await using (var session = await subject.OpenSessionAsync())
                                {
                                    Console.Write("<");
                                    await session.DequeueAsync();
                                    Interlocked.Increment(ref t2s);
                                    await session.FlushAsync();
                                    Console.Write(">");
                                }
                                await Task.Delay(0);
                            }
                        }
                        consumerDone.Set();
                    }
                    catch (Exception ex)
                    {
                        lastException = ex;
                        consumerDone.Set();
                    }
                }).Wait();
            });

            t1.Start();
            t2.Start();

            // Wait for producer to finish
            var producerFinished = producerDone.WaitOne(TimeSpan.FromSeconds(30));

            // Wait for consumer to finish
            var consumerFinished = consumerDone.WaitOne(TimeSpan.FromSeconds(30));

            if (!producerFinished)
            {
                Assert.Fail("Producer did not finish in time");
            }
            if (!consumerFinished)
            {
                Assert.Fail("Consumer did not finish in time");
            }
            if (lastException != null)
            {
                Assert.Fail(lastException.Message);
            }

            Assert.That(t1s, Is.EqualTo(target), "First thread did not enqueue expected amount.");
            Assert.That(t2s, Is.EqualTo(target), "Second thread did not dequeue expected amount.");
        }

        [Test]
        public async Task can_sequence_queues_on_separate_threads_with_size_limits()
        {
            var producerDone = new ManualResetEvent(false);
            var consumerDone = new ManualResetEvent(false);
            int t2s;
            var t1s = t2s = 0;
            const int target = 100;
            Exception? lastException = null;

            var t1 = new Thread(async () =>
            {
                try
                {

                    for (int i = 0; i < target; i++)
                    {
                        await using (var subject = await PersistentQueue.WaitForAsync("queue_tb", TimeSpan.FromSeconds(10)))
                        {
                            await using (var session = await subject.OpenSessionAsync())
                            {
                                Console.Write("(");
                                await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                                Interlocked.Increment(ref t1s);
                                await session.FlushAsync();
                                Console.Write(")");
                            }
                            await Task.Delay(0);
                        }
                    }
                    producerDone.Set();
                }
                catch (Exception ex)
                {
                    lastException = ex;
                    producerDone.Set();
                }
            });
            var t2 = new Thread(async ()=>
            {
                try
                {

                    for (int i = 0; i < target; i++)
                    {
                        await using (var subject = await PersistentQueue.WaitForAsync("queue_tb", TimeSpan.FromSeconds(10)))
                        {
                            await using (var session = await subject.OpenSessionAsync())
                            {
                                Console.Write("<");
                                await session.DequeueAsync();
                                Interlocked.Increment(ref t2s);
                                await session.FlushAsync();
                                Console.Write(">");
                            }
                            await Task.Delay(0);
                        }
                    }
                    consumerDone.Set();
                }
                catch (Exception ex)
                {
                    lastException = ex;
                    consumerDone.Set();
                }
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
            if (lastException != null)
            {
                Assert.Fail(lastException.Message);
            }

            Assert.That(t1s, Is.EqualTo(target));
            Assert.That(t2s, Is.EqualTo(target));
        }
    }
}