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

        /// <summary>
        /// Earlier version of test that didn't coordinate threads and tasks well.
        /// </summary>
        /// <returns></returns>
        [Test, Explicit]
        public async Task can_enqueue_and_dequeue_on_separate_threads()
        {
            var producerDone = new ManualResetEvent(false);
            var consumerDone = new ManualResetEvent(false);
            int t1s, t2s;
            t1s = t2s = 0;
            const int target = 100;
            var rnd = new Random();
            Exception? lastException = null;

            IPersistentQueue subject = await _factory.CreateAsync("queue_ta");
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
            var producerFinished = producerDone.WaitOne(TimeSpan.FromSeconds(50));

            // Wait for consumer to finish
            var consumerFinished = consumerDone.WaitOne(TimeSpan.FromSeconds(45));

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
        public async Task can_enqueue_and_dequeue_on_separate_threads_v2()
        {
            const int timeoutSeconds = 30;
            const int target = 100;

            var producerTcs = new TaskCompletionSource<bool>();
            var consumerTcs = new TaskCompletionSource<bool>();

            int enqueueCount = 0;
            int dequeueCount = 0;
            Exception? producerException = null;
            Exception? consumerException = null;

            IPersistentQueue subject = await _factory.CreateAsync("queue_ta");

            var producerThread = new Thread(() =>
            {
                Task.Run(async () =>
                {
                    try
                    {
                        var rnd = new Random();
                        for (int i = 0; i < target; i++)
                        {
                            await using (var session = await subject.OpenSessionAsync())
                            {
                                Console.Write("(");
                                await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                                Interlocked.Increment(ref enqueueCount);

                                await Task.Delay(rnd.Next(0, 50));

                                await session.FlushAsync();
                                Console.Write(")");
                            }
                        }

                        producerTcs.TrySetResult(true);
                    }
                    catch (Exception ex)
                    {
                        producerException = ex;
                        producerTcs.TrySetException(ex);
                    }
                }).ContinueWith(t =>
                {
                    if (t.IsFaulted)
                    {
                        producerException = t.Exception;
                        producerTcs.TrySetException(t.Exception ?? new Exception("Unknown producer error."));
                    }
                });
            })
            { IsBackground = true, Name = "Enqueueing Thread" };

            var consumerThread = new Thread(()=>
            {
                Task.Run(async () =>
                {
                    try
                    {
                        var rnd = new Random();
                        for (int i = 0; i < target; i++)
                        {
                            await using (var session = await subject.OpenSessionAsync())
                            {
                                Console.Write("<");
                                await session.DequeueAsync();
                                Interlocked.Increment(ref dequeueCount);
                                await Task.Delay(rnd.Next(0, 100));
                                await session.FlushAsync();
                                Console.Write(">");
                            }
                        }
                        consumerTcs.TrySetResult(true);
                    }
                    catch (Exception ex)
                    {
                        consumerException = ex;
                        consumerTcs.TrySetException(ex);
                    }
                }).ContinueWith(t =>
                {
                    if (t.IsFaulted)
                    {
                        consumerException = t.Exception?.InnerException;
                        consumerTcs.TrySetException(t.Exception ?? new Exception("Unknown consumer error."));
                    }
                });
            })
            { IsBackground = true, Name = "Dequeueing Thread" };

            producerThread.Start();
            consumerThread.Start();

            var timeoutTask = Task.Delay(TimeSpan.FromSeconds(timeoutSeconds));
            var allTasks = Task.WhenAll(
                Task.Run(async () => await producerTcs.Task),
                Task.Run(async () => await consumerTcs.Task));

            if (await Task.WhenAny(allTasks, timeoutTask) == timeoutTask)
            {
                // Handle timeout
                string status = $"Test timed out after {timeoutSeconds}s. Producer: {enqueueCount}/{target}, Consumer: {dequeueCount}/{target}";

                if (producerException != null)
                    status += $"\nProducer exception: {producerException}";

                if (consumerException != null)
                    status += $"\nConsumer exception: {consumerException}";

                Assert.Fail(status);
            }

            // Check for exceptions
            if (producerException != null)
                Assert.Fail($"Producer failed: {producerException}");

            if (consumerException != null)
                Assert.Fail($"Consumer failed: {consumerException}");

            await subject.DisposeAsync(); // I should just wrap everything above in an await using statement. If this is not called manually when instantiating the queue, it will use the synchronous Dispose method, which will (at this point intentionally) throw an exception in debug mode.

            // Verify counts
            Assert.That(enqueueCount, Is.EqualTo(target), "Producer didn't enqueue the expected number of items");
            Assert.That(dequeueCount, Is.EqualTo(target), "Consumer didn't dequeue the expected number of items");
        }


        [Test]
        public async Task can_sequence_queues_on_separate_threads()
        {
            var factory = _factory;// new PersistentQueueFactory();
            const int objectCount = 100;
            var random = new Random();
            int enqueueCount = 0, dequeueCount = 0;
            Exception? producerException = null, consumerException = null;

            // Create task completion sources to signal when threads are done
            var producerTcs = new TaskCompletionSource<bool>();
            var consumerTcs = new TaskCompletionSource<bool>();

            var producerThread = new Thread(async () =>
            {
                try
                {

                    for (int i = 0; i < objectCount; i++)
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
                        // Add jittery delay for each iteration
                        //int jitteryDelay = random.Next(5, 26);
                        //await Task.Delay(jitteryDelay);
                        await Task.Delay(2);
                    }
                    producerTcs.SetResult(true);
                }
                catch (Exception ex)
                {
                    producerException = ex;
                    producerTcs.SetException(ex);
                }
            })
            { IsBackground = true, Name = "Producer Thread" };

            var consumerThread = new Thread(async ()=>
            {
                try
                {
                    for (int i = 0; i < objectCount; i++)
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
                        await Task.Delay(5);
                    }
                    consumerTcs.SetResult(true);
                }
                catch (Exception ex)
                {
                    consumerException = ex;
                    consumerTcs.SetException(ex);
                }
            })
            { IsBackground = true, Name = "Consumer Thread" };

            producerThread.Start();
            consumerThread.Start();

            // Wait for both tasks to complete with timeout
            var timeoutTask = Task.Delay(TimeSpan.FromSeconds(300));
            var completionTask = Task.WhenAll(producerTcs.Task, consumerTcs.Task);

            if (await Task.WhenAny(completionTask, timeoutTask) == timeoutTask)
            {
                Assert.Fail("Test timed out.");
            }
            Console.WriteLine("");
            Console.WriteLine($"Producer completed: {enqueueCount}/{objectCount}, Consumer completed: {dequeueCount}/{objectCount}");

            if (producerException != null)
            {
                Assert.Fail($"Producer failed: {producerException}");
            }

            if (consumerException != null)
            {
                Assert.Fail($"Consumer failed: {consumerException}");
            }

            Assert.That(enqueueCount, Is.EqualTo(objectCount), "Producer thread did not enqueue expected number of items.");
            Assert.That(dequeueCount, Is.EqualTo(objectCount), "Consumer thread did not dequeue expected number of items.");
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
                        await using (var subject = await _factory.WaitForAsync("queue_tb", TimeSpan.FromSeconds(10)))
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
                        await using (var subject = await _factory.WaitForAsync("queue_tb", TimeSpan.FromSeconds(10)))
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