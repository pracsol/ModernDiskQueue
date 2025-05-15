namespace ModernDiskQueue.Tests
{
    using Microsoft.Extensions.Logging;
    using NSubstitute;
    using NUnit.Framework;
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    [TestFixture, SingleThreaded]
    public class ThreadSafeAccessTestsAsync
    {
        private IPersistentQueueFactory _factory = Substitute.For<IPersistentQueueFactory>();

        [SetUp]
        public void Setup()
        {
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.SetMinimumLevel(LogLevel.Warning);
                builder.AddConsole();
            });
            _factory = new PersistentQueueFactory(loggerFactory);
        }

        [Test]
        public async Task can_enqueue_and_dequeue_on_separate_threads()
        {
            const int TargetObjectCount = 100;
            Exception? lastProducerException = null;
            Exception? lastConsumerException = null;

            var enqueueCompleted = new ManualResetEventSlim(false);
            var dequeueCompleted = new ManualResetEventSlim(false);
            int timeoutForDequeueThreadInMinutes = 2;
            int timeoutForEnqueueThreadInMinutes = 2;

            int enqueueCount = 0;
            int dequeueCount = 0;

            IPersistentQueue q = await _factory.CreateAsync("queue_ta");

            var producerThread = new Thread(() =>
            {
                RunAsyncInThread(async () =>
                {
                    try
                    {
                        var rnd = new Random();
                        for (int i = 0; i < TargetObjectCount; i++)
                        {
                            await using (var session = await q.OpenSessionAsync())
                            {
                                //Console.Write("(");
                                await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                                //await Task.Delay(rnd.Next(0, 50));
                                await session.FlushAsync();
                                Interlocked.Increment(ref enqueueCount);
                                //Console.Write(")");
                            }
                            //await Task.Delay(rnd.Next(0, 10));
                        }
                    }
                    catch (Exception ex)
                    {
                        lastProducerException = ex;
                    }
                    finally
                    {
                        enqueueCompleted.Set();
                    }
                });
            })
            { IsBackground = true, Name = "Enqueueing Thread" };

            var consumerThread = new Thread(()=>
            {
                RunAsyncInThread(async () =>
                {
                    try
                    {
                        var rnd = new Random();
                        do
                        {
                            await using (var session = await q.OpenSessionAsync())
                            {
                                //Console.Write("<");
                                var obj = await session.DequeueAsync();
                                if (obj != null)
                                {
                                    await session.FlushAsync();
                                    Interlocked.Increment(ref dequeueCount);
                                    //Console.Write(">");
                                }
                                else
                                {
                                    Console.WriteLine("got nothing");
                                }
                                await Task.Delay(rnd.Next(5, 20));
                            }
                        }
                        while (dequeueCount < TargetObjectCount);
                    }
                    catch (Exception ex)
                    {
                        lastConsumerException = ex;
                    }
                    finally
                    {
                        dequeueCompleted.Set();
                    }
                });
            })
            { IsBackground = true, Name = "Dequeueing Thread" };

            producerThread.Start();
            consumerThread.Start();


            if (!enqueueCompleted.Wait(TimeSpan.FromMinutes(timeoutForEnqueueThreadInMinutes)))
            {
                Console.WriteLine("Producer (enqueue) thread timed out.");
            }
            if (!dequeueCompleted.Wait(TimeSpan.FromMinutes(timeoutForDequeueThreadInMinutes)))
            {
                Console.WriteLine("Consumer (dequeue) thread timed out.");
            }

            if (lastProducerException != null)
            {
                Assert.Fail($"Producer (enqueue) thread terminated early with error {lastProducerException.Message}");
            }

            if (lastConsumerException!= null)
            {
                Assert.Fail($"Consumer (dequeue) thread terminated early with error {lastConsumerException.Message}");
            }

            await q.DisposeAsync();

            // Verify counts
            Assert.That(enqueueCount, Is.EqualTo(TargetObjectCount), "Producer didn't enqueue the expected number of items");
            Assert.That(dequeueCount, Is.EqualTo(TargetObjectCount), "Consumer didn't dequeue the expected number of items");
        }

        [Test]
        public async Task can_sequence_queues_on_separate_threads()
        {
            var factory = _factory;
            const int TargetObjectCount = 100;
            var random = new Random();
            int enqueueCount = 0, dequeueCount = 0;
            Exception? producerException = null, consumerException = null;

            var enqueueCompleted = new ManualResetEventSlim(false);
            var dequeueCompleted = new ManualResetEventSlim(false);
            int timeoutForDequeueThreadInMinutes = 2;
            int timeoutForEnqueueThreadInMinutes = 2;

            var producerThread = new Thread(() =>
            {
                RunAsyncInThread(async () =>
                {
                    try
                    {
                        for (int i = 0; i < TargetObjectCount; i++)
                        {
                            await using (var queue = await factory.WaitForAsync("queue_tb", TimeSpan.FromSeconds(30)).ConfigureAwait(false))
                            {
                                await using (var session = await queue.OpenSessionAsync().ConfigureAwait(false))
                                {
                                    Console.Write("(");
                                    await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
                                    Interlocked.Increment(ref enqueueCount);
                                    await session.FlushAsync().ConfigureAwait(false);
                                    Console.Write(")");
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        producerException = ex;
                        throw;
                    }
                    finally
                    {
                        enqueueCompleted.Set();
                    }
                });
            })
            { IsBackground = true, Name = "Producer Thread" };

            var consumerThread = new Thread(()=>
            {
                RunAsyncInThread(async () =>
                {
                    try
                    {
                        for (int i = 0; i < TargetObjectCount; i++)
                        {
                            await using (var queue = await factory.WaitForAsync("queue_tb", TimeSpan.FromSeconds(30)).ConfigureAwait(false))
                            {
                                await using (var session = await queue.OpenSessionAsync().ConfigureAwait(false))
                                {
                                    Console.Write("<");
                                    await session.DequeueAsync().ConfigureAwait(false);
                                    Interlocked.Increment(ref dequeueCount);
                                    await session.FlushAsync().ConfigureAwait(false);
                                    Console.Write(">");
                                }
                            }
                            // Add jittery delay for each iteration
                            //int jitteryDelay = random.Next(5, 26);
                            //await Task.Delay(5);
                        }
                    }
                    catch (Exception ex)
                    {
                        consumerException = ex;
                        throw;
                    }
                    finally
                    {
                        dequeueCompleted.Set();
                    }
                });
            })
            { IsBackground = true, Name = "Consumer Thread" };

            producerThread.Start();
            consumerThread.Start();

            if (!enqueueCompleted.Wait(TimeSpan.FromMinutes(timeoutForEnqueueThreadInMinutes)))
            {
                Console.WriteLine("Enqueue thread timed out.");
            }
            if (!dequeueCompleted.Wait(TimeSpan.FromMinutes(timeoutForDequeueThreadInMinutes)))
            {
                Console.WriteLine("Enqueue thread timed out.");
            }

            if (producerException != null)
            {
                Assert.Fail($"Producer thread terminated early with error {producerException.Message}");
            }

            if (consumerException != null)
            {
                Assert.Fail($"Consumer thread terminated early with error {consumerException.Message}");
            }

            Assert.That(enqueueCount, Is.EqualTo(TargetObjectCount), "Producer thread did not enqueue expected number of items.");
            Assert.That(dequeueCount, Is.EqualTo(TargetObjectCount), "Consumer thread did not dequeue expected number of items.");
        }

        [Test]
        public async Task can_sequence_queues_on_separate_threads_with_size_limits()
        {
            var enqueueCompleted = new ManualResetEventSlim(false);
            var dequeueCompleted = new ManualResetEventSlim(false);
            int timeoutForDequeueThreadInMinutes = 2;
            int timeoutForEnqueueThreadInMinutes = 2;
            Exception? producerException = null, consumerException = null;
            int t2s;
            int t1s = t2s = 0;
            const int target = 100;
            Exception? lastException = null;

            var producerThread = new Thread(() =>
            {
                RunAsyncInThread(async () =>
                {
                    try
                    {
                        for (int i = 0; i < target; i++)
                        {
                            await using (var q = await _factory.WaitForAsync("queue_tb", TimeSpan.FromSeconds(10)))
                            {
                                await using (var session = await q.OpenSessionAsync())
                                {
                                    Console.Write("(");
                                    await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                                    Interlocked.Increment(ref t1s);
                                    await session.FlushAsync();
                                    Console.Write(")");
                                }
                                //await Task.Delay(0);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        producerException = ex;
                        throw;
                    }
                    finally
                    {
                        enqueueCompleted.Set();
                    }
                });
            })
            { IsBackground = true, Name = "Producer Thread" };


            var consumerThread = new Thread(()=>
            {
                RunAsyncInThread(async () =>
                {
                    try
                    {

                        for (int i = 0; i < target; i++)
                        {
                            await using (var q = await _factory.WaitForAsync("queue_tb", TimeSpan.FromSeconds(10)))
                            {
                                await using (var session = await q.OpenSessionAsync())
                                {
                                    Console.Write("<");
                                    await session.DequeueAsync();
                                    Interlocked.Increment(ref t2s);
                                    await session.FlushAsync();
                                    Console.Write(">");
                                }
                                //await Task.Delay(0);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        consumerException = ex;
                        throw;
                    }
                    finally
                    {
                        dequeueCompleted.Set();
                    }
                });
            })
            { IsBackground = true, Name = "Consumer Thread" };

            producerThread.Start();
            consumerThread.Start();

            if (!enqueueCompleted.Wait(TimeSpan.FromMinutes(timeoutForEnqueueThreadInMinutes)))
            {
                Console.WriteLine("Enqueue thread timed out.");
            }
            if (!dequeueCompleted.Wait(TimeSpan.FromMinutes(timeoutForDequeueThreadInMinutes)))
            {
                Console.WriteLine("Enqueue thread timed out.");
            }

            if (producerException != null)
            {
                Assert.Fail($"Producer thread terminated early with error {producerException.Message}");
            }

            if (consumerException != null)
            {
                Assert.Fail($"Consumer thread terminated early with error {consumerException.Message}");
            }

            Assert.That(t1s, Is.EqualTo(target));
            Assert.That(t2s, Is.EqualTo(target));
        }

        public void RunAsyncInThread(Func<Task> asyncFunc)
        {
            var t = asyncFunc();
            t.GetAwaiter().GetResult();
        }
    }
}