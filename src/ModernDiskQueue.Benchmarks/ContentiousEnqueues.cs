namespace ModernDiskQueue.Benchmarks
{
    using BenchmarkDotNet.Attributes;
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections.Concurrent;
    using System.Text;
    using System.Threading.Tasks;
    using ModernDiskQueue;
    using System.Diagnostics;

    [Config(typeof(LongRunningTestConfig))]
    public class ContentiousEnqueues
    {
        private PersistentQueueFactory  _factory;
        private const string QueuePath = "AsyncEnqueue";
        public event Action<int>? ProgressUpdated;
        private static int _progressCounter = 0;

        [GlobalSetup]
        public void Setup()
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
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            Helpers.AttemptManualCleanup(QueuePath);
        }

        [IterationSetup]
        public void IterationSetup()
        {
            Helpers.AttemptManualCleanup(QueuePath);
        }

        [Benchmark]
        public async Task AsyncReadHeavyMultithread()
        {
            ProgressUpdated += progress => Console.WriteLine($"Progress: {progress} items processed.");

            // Thread-specific retry counts for exponential backoff
            var threadRetries = new ConcurrentDictionary<int, int>();

            // Arrange test parameters
            int numberOfObjectsToEnqueue = 500;
            int numberOfDequeueThreads = 10;
            int enqueueHeadstartInSeconds = 18;
            int timeoutForQueueCreationDuringDequeueInSeconds = 160;
            int timeoutForQueueCreationDuringEnqueueInSeconds = 160;
            int timeoutForDequeueThreadsInMinutes = 5;
            int timeoutForEnqueueThreadInMinutes = 5;
            var successfulThreads = 0;
            var failedThreads = new ConcurrentBag<(int threadId, string reason)>();

            var enqueueCompleted = new ManualResetEventSlim(false);
            var dequeueCompletedEvents = new ManualResetEvent[numberOfDequeueThreads];

            for (int i = 0; i < dequeueCompletedEvents.Length; i++)
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

                        for (int i = 0; i < numberOfObjectsToEnqueue; i++)
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
                                    //Console.WriteLine($"Thread {Environment.CurrentManagedThreadId} is enqueueing");
                                    await s.EnqueueAsync(Encoding.ASCII.GetBytes($"Enqueued item {i}"));
                                    await s.FlushAsync();
                                }
                            }
                            //await Task.Delay(5);
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
                IsBackground = true
            };

            enqueueThread.Start();

            // Wait for the enqueue thread to signal completion or for 18 seconds to pass
            if (!enqueueCompleted.Wait(TimeSpan.FromSeconds(enqueueHeadstartInSeconds)))
            {
                Console.WriteLine($"Warning: Enqueue thread did not complete within {enqueueHeadstartInSeconds} seconds.");
            }

            var rnd = new Random();
            var threads = new Thread[numberOfDequeueThreads];

            try
            {
                // dequeue threads
                for (int i = 0; i < numberOfDequeueThreads; i++)
                {
                    int threadIndex = i;
                    var completionEvent = dequeueCompletedEvents[threadIndex];

                    threads[i] = new Thread(() =>
                    {
                        var count = numberOfObjectsToEnqueue/numberOfDequeueThreads;
                        try
                        {
                            RunAsyncInThread(async () =>
                            {
                                var threadId = Environment.CurrentManagedThreadId;

                                while (count > 0)
                                {
                                    // Smart backoff for lock acquisition
                                    var retryCount = threadRetries.AddOrUpdate(threadId, 0, (_, c) => c + 1);
                                    if (retryCount > 0)
                                    {
                                        // Exponential backoff with jitter
                                        var backoffTime = Math.Min(50 * Math.Pow(1.5, Math.Min(retryCount, 10)) + rnd.Next(50), 2000);
                                        //await Task.Delay((int)backoffTime);
                                    }

                                    try
                                    {

                                        await using var q = await _factory.WaitForAsync(QueuePath, TimeSpan.FromSeconds(timeoutForQueueCreationDuringDequeueInSeconds));
                                        {
                                            await using var s = await q.OpenSessionAsync();
                                            {
                                                var data = await s.DequeueAsync();
                                                Console.WriteLine($"Thread {threadId} dequeued data item {(numberOfObjectsToEnqueue / numberOfDequeueThreads) - count + 1}");

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
                                        //await Task.Delay(20 + rnd.Next(30));
                                    }
                                    catch (TimeoutException)
                                    {
                                        // Increment retry count for next attempt
                                        threadRetries.AddOrUpdate(threadId, 1, (_, c) => c + 1);
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
                            Console.WriteLine($"Thread {threadIndex} completed {(numberOfObjectsToEnqueue / numberOfDequeueThreads) - count} dequeues.");
                        }
                    })
                    { IsBackground = true };
                    threads[i].Start();
                }

                for (int i = 0; i < numberOfDequeueThreads; i++)
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
            }
            finally
            {
                // Clean up resources
                for (int i = 0; i < dequeueCompletedEvents.Length; i++)
                {
                    dequeueCompletedEvents[i].Dispose();
                }
                enqueueCompleted.Dispose();
            }
        }

        [Benchmark]
        public void SyncReadHeavyMultithread()
        {
            // Arrange test parameters
            int numberOfObjectsToEnqueue = 500;
            int numberOfDequeueThreads = 10;
            int enqueueHeadstartInSeconds = 18;
            int timeoutForQueueCreationDuringDequeueInSeconds = 100;
            int timeoutForQueueCreationDuringEnqueueInSeconds = 50;
            int timeoutForDequeueThreadsInMinutes = 3;
            int timeoutForEnqueueThreadInMinutes = 3;
            var totalDequeues = 0;
            var successfulThreads = 0;
            var failedThreads = new ConcurrentBag<(int threadId, string reason)>();

            DateTime testStartTime = DateTime.Now;
            using (var queue = new PersistentQueue(QueuePath))
            {
                queue.HardDelete(false);
            }

            var enqueueCompleted = new ManualResetEventSlim(false);
            var dequeueCompletedEvents = new ManualResetEvent[numberOfDequeueThreads];

            for (int i = 0; i < dequeueCompletedEvents.Length; i++)
            {
                dequeueCompletedEvents[i] = new ManualResetEvent(false);
            }

            // enqueue thread
            var enqueueThread = new Thread(() =>
            {
                var threadId = Environment.CurrentManagedThreadId;

                try
                {
                    for (int i = 0; i < numberOfObjectsToEnqueue; i++)
                    {
                        using var q = PersistentQueue.WaitFor(QueuePath, TimeSpan.FromSeconds(timeoutForQueueCreationDuringEnqueueInSeconds));
                        {
                            using (var s = q.OpenSession())
                            {
                                s.Enqueue(Encoding.ASCII.GetBytes($"Enqueued item {i}"));
                                s.Flush();
                            }
                        }
                    }
                    Console.WriteLine($"Enqueue thread finished.");
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
                IsBackground = true
            };

            enqueueThread.Start();

            // Wait for the enqueue thread to signal completion or for 18 seconds to pass
            if (!enqueueCompleted.Wait(TimeSpan.FromSeconds(enqueueHeadstartInSeconds)))
            {
                Console.WriteLine($"Warning: Enqueue thread did not complete within {enqueueHeadstartInSeconds} second headstart.");
            }

            var rnd = new Random();
            var threads = new Thread[numberOfDequeueThreads];

            try
            {
                // dequeue threads
                for (int i = 0; i < threads.Length; i++)
                {
                    var completionEvent = dequeueCompletedEvents[i];
                    threads[i] = new Thread(() =>
                    {
                        var threadIndex = Environment.CurrentManagedThreadId;
                        var count = numberOfObjectsToEnqueue/numberOfDequeueThreads;

                        try
                        {
                            while (count > 0)
                            {
                                Thread.Sleep(rnd.Next(5));

                                using var q = PersistentQueue.WaitFor(QueuePath, TimeSpan.FromSeconds(timeoutForQueueCreationDuringDequeueInSeconds));
                                {
                                    using var s = q.OpenSession();
                                    {
                                        var data = s.Dequeue();

                                        if (data != null)
                                        {
                                            count--;
                                            s.Flush();
                                        }
                                    }
                                }
                            }
                            Interlocked.Increment(ref successfulThreads);
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
                        }
                    })
                    { IsBackground = true };
                    threads[i].Start();
                }

                for (int e = 0; e < threads.Length; e++)
                {
                    if (!dequeueCompletedEvents[e].WaitOne(TimeSpan.FromMinutes(timeoutForDequeueThreadsInMinutes)))
                    {
                        failedThreads.Add((e, "timeout"));
                    }
                }

                if (!enqueueCompleted.Wait(TimeSpan.FromMinutes(timeoutForEnqueueThreadInMinutes)))
                {
                    Console.WriteLine("Enqueue thread timed out.");
                }
            }
            finally
            {
                // Clean up resources
                for (int i = 0; i < dequeueCompletedEvents.Length; i++)
                {
                    dequeueCompletedEvents[i].Dispose();
                }
                enqueueCompleted.Dispose();
            }
        }

        public void RunAsyncInThread(Func<Task> asyncFunc)
        {
            var t = asyncFunc();
            t.GetAwaiter().GetResult();
        }
    }
}