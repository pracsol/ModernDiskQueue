// <copyright file="PerformanceTestsAsync.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>

namespace ModernDiskQueue.Tests
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using NSubstitute;
    using NUnit.Framework;

    [TestFixture]
    [Explicit]
    [SingleThreaded]
    public class PerformanceTestsAsync : PersistentQueueTestsBase
    {
        private const int LargeCount = 1000000;
        private const int SmallCount = 500;

        private static int _progressCounter = 0;

        public event Action<int>? ProgressUpdated;

        protected override string QueuePath => "PerformanceTests";

        private IPersistentQueueFactory _factory = Substitute.For<IPersistentQueueFactory>();

        [SetUp]
        public new void Setup()
        {
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.SetMinimumLevel(LogLevel.Warning);
                builder.AddSimpleConsole(c =>
                {
                    c.TimestampFormat = "[HH:mm:ss:ffff] ";
                });
            });
            _factory = new PersistentQueueFactory(loggerFactory);
            base.Setup();
        }

        [Test]
        [Description(
            "With a mid-range SSD, this is some 20x slower " +
            "than with a single flush (depends on disk speed)")]
        public async Task Enqueue_million_items_with_100_flushes()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath).ConfigureAwait(false))
            {
                for (int i = 0; i < 100; i++)
                {
                    await using (var session = await queue.OpenSessionAsync().ConfigureAwait(false))
                    {
                        for (int j = 0; j < 10000; j++)
                        {
                            await session.EnqueueAsync(Guid.NewGuid().ToByteArray());

                            // await Task.Yield();
                        }

                        await session.FlushAsync();
                    }

                    await Task.Yield();
                }
            }
        }

        [Test]
        public async Task Enqueue_million_items_with_single_flush()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int i = 0; i < LargeCount; i++)
                    {
                        await session.EnqueueAsync(Guid.NewGuid().ToByteArray());
                    }

                    await session.FlushAsync();
                }
            }
        }

        [Test]
        public async Task write_heavy_multi_thread_workload()
        {
            const int NumberOfObjects = 1000;
            const int NumberOfEnqueueThreads = 100;
            const int TimeoutForQueueCreationDuringEnqueueInSeconds = 100;
            const int TimeoutForQueueCreationDuringDequeueInSeconds = 50;
            const int TimeoutForDequeueThreadInMinutes = 3;
            const int TimeoutForEnqueueThreadsInMinutes = 3;

            // Thread-specific retry counts for exponential backoff
            var threadRetries = new ConcurrentDictionary<int, int>();

            DateTime testStartTime = DateTime.Now;
            int successfulThreads = 0;
            int objectsLeftToDequeue = NumberOfObjects;
            ConcurrentBag<(int ThreadId, string Reason)> failedThreads = [];

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await queue.HardDeleteAsync(false);
            }

            var enqueueCompletedEvents = new ManualResetEvent[NumberOfEnqueueThreads];
            var dequeueCompleted = new ManualResetEventSlim(false);

            for (int i = 0; i < NumberOfEnqueueThreads; i++)
            {
                enqueueCompletedEvents[i] = new ManualResetEvent(false);
            }

            var rnd = new Random();
            var threads = new Thread[NumberOfEnqueueThreads];
            DateTime enqueueStartTime = DateTime.Now;

            try
            {
                // enqueue threads
                for (int i = 0; i < NumberOfEnqueueThreads; i++)
                {
                    int threadIndex = i;
                    var completionEvent = enqueueCompletedEvents[threadIndex];

                    threads[i] = new Thread(() =>
                    {
                        var count = NumberOfObjects / NumberOfEnqueueThreads;
                        try
                        {
                            RunAsyncInThread(async () =>
                            {
                                var threadId = Environment.CurrentManagedThreadId;

                                var countOfObjectsToEnqueue = NumberOfObjects / NumberOfEnqueueThreads;
                                while (countOfObjectsToEnqueue > 0)
                                {
                                    try
                                    {
                                        // await Task.Delay(rnd.Next(5));
                                        await using (var q = await _factory.WaitForAsync(QueuePath, TimeSpan.FromSeconds(TimeoutForQueueCreationDuringEnqueueInSeconds)))
                                        {
                                            await using (var s = await q.OpenSessionAsync())
                                            {
                                                await s.EnqueueAsync(Encoding.ASCII.GetBytes($"Thread {threadId} enqueue {threadIndex}"));
                                                await s.FlushAsync();
                                                countOfObjectsToEnqueue--;
                                            }
                                        }
                                    }
                                    catch (TimeoutException)
                                    {
                                        throw;
                                    }
                                }

                                _ = Interlocked.Increment(ref successfulThreads);
                            });
                        }
                        catch (TimeoutException ex)
                        {
                            Console.WriteLine($"Thread {threadIndex} timed out trying to create the queue: {ex}");
                            failedThreads.Add((threadIndex, ex.Message));
                        }
                        catch (Exception ex)
                        {
                            failedThreads.Add((threadIndex, ex.Message));
                        }
                        finally
                        {
                            completionEvent.Set();
                            Console.WriteLine($"Thread {threadIndex} completed {(NumberOfObjects / NumberOfEnqueueThreads) - count} enqueues.");
                        }
                    })
                    { IsBackground = true };
                    threads[i].Start();
                }

                // put the dequeue single thread here
                var dequeueThread = new Thread(() =>
                {
                    try
                    {
                        RunAsyncInThread(async () =>
                        {
                            var threadId = Environment.CurrentManagedThreadId;
                            while (objectsLeftToDequeue > 0)
                            {
                                // Smart backoff for lock acquisition
                                var retryCount = threadRetries.AddOrUpdate(threadId, 0, (_, count) => count);

                                byte[]? bytes;

                                await using (var q = await _factory.WaitForAsync(QueuePath, TimeSpan.FromSeconds(TimeoutForQueueCreationDuringDequeueInSeconds)))
                                {
                                    // Reset retry count on success
                                    threadRetries[threadId] = 0;

                                    await using (var s = await q.OpenSessionAsync())
                                    {
                                        bytes = await s.DequeueAsync();
                                        if (bytes is not null)
                                        {
                                            await s.FlushAsync();
                                            Interlocked.Decrement(ref objectsLeftToDequeue);
                                        }
                                        else
                                        {
                                            if (retryCount > 0)
                                            {
                                                Console.WriteLine("Data was null so backing off.");
                                                var backoffTime = Math.Min(50 * Math.Pow(1.5, Math.Min(retryCount, 10)), 2000);
                                                await Task.Delay((int)backoffTime);
                                            }
                                        }
                                    }
                                }
                            }

                            Console.WriteLine($"Dequeue thread finished");
                        });
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Dequeue thread failed: {ex.Message}");
                    }
                    finally
                    {
                        dequeueCompleted.Set();
                    }
                })
                {
                    IsBackground = true,
                };

                dequeueThread.Start();

                // now let it all go
                for (int i = 0; i < NumberOfEnqueueThreads; i++)
                {
                    if (!enqueueCompletedEvents[i].WaitOne(TimeSpan.FromMinutes(TimeoutForEnqueueThreadsInMinutes)))
                    {
                        failedThreads.Add((i, "timeout"));
                    }
                }

                if (!dequeueCompleted.Wait(TimeSpan.FromMinutes(TimeoutForDequeueThreadInMinutes)))
                {
                    Console.WriteLine("Dequeue thread timed out.");
                }

                Console.WriteLine($"All enqueue threads finished, took {(DateTime.Now - enqueueStartTime).TotalSeconds} seconds.");
            }
            finally
            {
                // Clean up resources
                for (int i = 0; i < enqueueCompletedEvents.Length; i++)
                {
                    enqueueCompletedEvents[i].Dispose();
                }

                dequeueCompleted.Dispose();
                Console.WriteLine($"Total test time took {(DateTime.Now - testStartTime).TotalSeconds} seconds.");
            }

            Assert.That(objectsLeftToDequeue, Is.EqualTo(0), "Did not receive all messages");
        }

        /// <summary>
        /// This test simulates a read-heavy workload with multiple threads concurrenty dequeuing items from the queue.
        /// Just as with the Enqueue_million_items_with_100_flushes test, this is heavily dependent on the host
        /// system performance. To be able to complete in time, one may need to adjust the sleep duration before
        /// the dequeue operation starts in order to give the enqueue operation more of a head start. One thread
        /// performing 1000 write operations doesn't complete as quickly as 100 threads doing read operations.
        /// To some extent the read_heavy and write_heavy tests can be used as performance profilers if you
        /// know the demand that will be placed on the queue and can run the tests on a system with a similar
        /// performance profile as your deployment target.
        /// </summary>
        [Test]
        public async Task read_heavy_multi_thread_workload()
        {
            // Thread-specific retry counts for exponential backoff
            var threadRetries = new ConcurrentDictionary<int, int>();

            const int NumberOfObjects = 1000;
            const int NumberOfDequeueThreads = 100;
            int enqueueHeadstartInSeconds = 18;
            int timeoutForQueueCreationDuringDequeueInSeconds = 100;
            int timeoutForQueueCreationDuringEnqueueInSeconds = 50;
            int timeoutForDequeueThreadsInMinutes = 3;
            int timeoutForEnqueueThreadInMinutes = 3;
            DateTime testStartTime = DateTime.Now;
            int successfulThreads = 0;
            ConcurrentBag<(int ThreadId, string Reason)> failedThreads = [];

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await queue.HardDeleteAsync(false);
            }

            var enqueueCompleted = new ManualResetEventSlim(false);
            var dequeueCompletedEvents = new ManualResetEvent[NumberOfDequeueThreads];

            for (int i = 0; i < NumberOfDequeueThreads; i++)
            {
                dequeueCompletedEvents[i] = new ManualResetEvent(false);
            }

            // enqueue thread
            var enqueueThread = new Thread(() =>
            {
                try
                {
                    RunAsyncInThread(async () =>
                    {
                        var threadId = Environment.CurrentManagedThreadId;

                        for (int i = 0; i < NumberOfObjects; i++)
                        {
                            Interlocked.Increment(ref _progressCounter);

                            // Smart backoff for lock acquisition
                            var retryCount = threadRetries.AddOrUpdate(threadId, 0, (_, count) => count);
                            if (retryCount > 0)
                            {
                                var backoffTime = Math.Min(50 * Math.Pow(1.5, Math.Min(retryCount, 10)), 2000);
                                await Task.Delay((int)backoffTime);
                            }

                            await using (var q = await _factory.WaitForAsync(QueuePath, TimeSpan.FromSeconds(timeoutForQueueCreationDuringEnqueueInSeconds)))
                            {
                                // Reset retry count on success
                                threadRetries[threadId] = 0;

                                await using (var s = await q.OpenSessionAsync())
                                {
                                    // Console.WriteLine($"Thread {Environment.CurrentManagedThreadId} is enqueueing");
                                    await s.EnqueueAsync(Encoding.ASCII.GetBytes($"Enqueued item {i}"));
                                    await s.FlushAsync();
                                }
                            }

                            // await Task.Delay(5);
                        }

                        Console.WriteLine($"Enqueue thread finished");
                    });
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Enqueue thread failed: {ex.Message}");
                }
                finally
                {
                    enqueueCompleted.Set();
                }
            })
            {
                IsBackground = true,
            };

            enqueueThread.Start();

            // enqueueThread.Join(); // wait for the enqueue thread to finish
            // If we don't wait for the enqueue thread to complete, the dequeue thread will start over top of it.
            // The dequeue threads will quickly outpace the writing (enqueue) thread if enough head start isn't
            // given (thread.sleep). This also depends greatly on disk performance. Instead of doing a single flush
            // on the writes, a flush per enqueue is going to be very slow. But if this test is to simulate
            // a very active concurrent environment with more readers than writers, this may be a good way to
            // understand the performance limitations and characteristics.

            // Wait for the enqueue thread to signal completion or for 18 seconds to pass
            if (!enqueueCompleted.Wait(TimeSpan.FromSeconds(enqueueHeadstartInSeconds)))
            {
                Console.WriteLine($"Warning: Enqueue thread did not complete within {enqueueHeadstartInSeconds} seconds.");
            }

            var rnd = new Random();
            var threads = new Thread[NumberOfDequeueThreads];
            DateTime dequeueStartTime = DateTime.Now;

            try
            {
                // dequeue threads
                for (int i = 0; i < NumberOfDequeueThreads; i++)
                {
                    int threadIndex = i;
                    var completionEvent = dequeueCompletedEvents[threadIndex];

                    threads[i] = new Thread(() =>
                    {
                        var count = NumberOfObjects / NumberOfDequeueThreads;
                        try
                        {
                            RunAsyncInThread(async () =>
                            {
                                var threadId = Environment.CurrentManagedThreadId;

                                while (count > 0)
                                {
                                    try
                                    {
                                        await using var q = await _factory.WaitForAsync(QueuePath, TimeSpan.FromSeconds(timeoutForQueueCreationDuringDequeueInSeconds));
                                        {
                                            await using var s = await q.OpenSessionAsync();
                                            {
                                                var data = await s.DequeueAsync();

                                                // Console.WriteLine($"Thread {threadId} dequeued data item {(NumberOfObjects / NumberOfDequeueThreads) - count + 1}");
                                                if (data != null && data.Length > 0)
                                                {
                                                    count--;
                                                    await s.FlushAsync();
                                                }
                                                else
                                                {
                                                    Console.WriteLine("Data was null");
                                                }
                                            }
                                        }

                                        // Add a small intentional pause between operations
                                        // to reduce the chance that the same thread re-acquires the lock instantly
                                        // await Task.Delay(20 + rnd.Next(30));
                                    }
                                    catch (TimeoutException)
                                    {
                                        throw;
                                    }
                                }

                                _ = Interlocked.Increment(ref successfulThreads);
                            });
                        }
                        catch (TimeoutException ex)
                        {
                            Console.WriteLine($"Thread {threadIndex} timed out trying to create the queue: {ex}");
                            failedThreads.Add((threadIndex, ex.Message));
                        }
                        catch (Exception ex)
                        {
                            failedThreads.Add((threadIndex, ex.Message));
                        }
                        finally
                        {
                            completionEvent.Set();
                            Console.WriteLine($"Thread {threadIndex} completed {(NumberOfObjects / NumberOfDequeueThreads) - count} dequeues.");
                        }
                    })
                    { IsBackground = true };
                    threads[i].Start();
                }

                for (int i = 0; i < NumberOfDequeueThreads; i++)
                {
                    if (!dequeueCompletedEvents[i].WaitOne(TimeSpan.FromMinutes(timeoutForDequeueThreadsInMinutes)))
                    {
                        failedThreads.Add((i, "timeout"));
                    }
                }

                if (!enqueueCompleted.Wait(TimeSpan.FromMinutes(timeoutForEnqueueThreadInMinutes)))
                {
                    Console.WriteLine("Enqueue thread timed out.");
                }

                Console.WriteLine($"All dequeue threads finished, took {(DateTime.Now - dequeueStartTime).TotalSeconds} seconds.");
            }
            finally
            {
                // Clean up resources
                for (int i = 0; i < dequeueCompletedEvents.Length; i++)
                {
                    dequeueCompletedEvents[i].Dispose();
                }

                enqueueCompleted.Dispose();
                Console.WriteLine($"Total test time took {(DateTime.Now - testStartTime).TotalSeconds} seconds.");
            }
        }

        [Test]
        public async Task Enqueue_and_dequeue_million_items_same_queue()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int i = 0; i < LargeCount; i++)
                    {
                        await session.EnqueueAsync(Guid.NewGuid().ToByteArray());
                    }

                    await session.FlushAsync();
                }

                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int i = 0; i < LargeCount; i++)
                    {
                        Ignore();
                    }

                    await session.FlushAsync();
                }
            }
        }

        [Test]
        public async Task Enqueue_and_dequeue_million_items_restart_queue()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    int threadId = Environment.CurrentManagedThreadId;
                    for (int i = 0; i < LargeCount; i++)
                    {
                        if (threadId != Environment.CurrentManagedThreadId)
                        {
                            Console.WriteLine($"Changed to enqueuing on thread {Environment.CurrentManagedThreadId}.");
                            threadId = Environment.CurrentManagedThreadId;
                        }

                        await session.EnqueueAsync(Guid.NewGuid().ToByteArray());
                    }

                    await session.FlushAsync();
                }
            }

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    int threadId = Environment.CurrentManagedThreadId;
                    for (int i = 0; i < LargeCount; i++)
                    {
                        if (threadId != Environment.CurrentManagedThreadId)
                        {
                            Console.WriteLine($"Changed to DEqueuing on thread {Environment.CurrentManagedThreadId}.");
                            threadId = Environment.CurrentManagedThreadId;
                        }

                        _ = await session.DequeueAsync();
                    }

                    await session.FlushAsync();
                }
            }
        }

        [Test]
        public async Task Enqueue_and_dequeue_large_items_with_restart_queue()
        {
            var random = new Random();
            var itemsSizes = new List<int>();
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int i = 0; i < SmallCount; i++)
                    {
                        var data = new byte[random.Next(1024 * 512, 1024 * 1024)];
                        itemsSizes.Add(data.Length);
                        await session.EnqueueAsync(data);
                    }

                    await session.FlushAsync();
                }
            }

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int i = 0; i < SmallCount; i++)
                    {
                        var dequeued = await session.DequeueAsync();
                        Assert.That(itemsSizes[i], Is.EqualTo(dequeued?.Length ?? -1));
                    }

                    await session.FlushAsync();
                }
            }
        }

        private static void Ignore()
        {
        }

        private void RunAsyncInThread(Func<Task> asyncFunc)
        {
            var t = asyncFunc();
            t.GetAwaiter().GetResult();
        }
    }
}