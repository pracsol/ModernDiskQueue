namespace ModernDiskQueue.Benchmarks
{
    using BenchmarkDotNet.Attributes;
    using Microsoft.Extensions.Logging;
    using System;
    using ModernDiskQueue;
    using System.Threading.Tasks;

    [Config(typeof(BenchmarkConfigThreadTaskComparison))]
    public class ThreadsAndTasks
    {
        private PersistentQueueFactory _factory;
        private const int CountOfObjectsToEnqueue = 100;
        private const string QueuePathForAsyncThreads = "AsyncThreadBased";
        private const string QueuePathForAsyncTasks = "AsyncTaskBased";
        private const string QueuePathForSyncThreads = "SyncThreadBased";

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
            Helpers.AttemptManualCleanup(QueuePathForAsyncThreads);
            Helpers.AttemptManualCleanup(QueuePathForAsyncTasks);
            Helpers.AttemptManualCleanup(QueuePathForSyncThreads);
        }

        [IterationSetup]
        public void IterationSetup()
        {
            Helpers.AttemptManualCleanup(QueuePathForAsyncThreads);
            Helpers.AttemptManualCleanup(QueuePathForAsyncTasks);
            Helpers.AttemptManualCleanup(QueuePathForSyncThreads);
        }

        [Benchmark]
        public void SyncEnqueueDequeueConcurrentlyWithThreads()
        {
            const int TargetObjectCount = CountOfObjectsToEnqueue;
            int enqueueCount = 0, dequeueCount = 0;
            var rnd = new Random();

            IPersistentQueue q = new PersistentQueue(QueuePathForSyncThreads);

            var producerThread = new Thread(() =>
            {
                try
                {
                    for (int i = 0; i < TargetObjectCount; i++)
                    {
                        using (var session = q.OpenSession())
                        {
                            session.Enqueue(new byte[] { 1, 2, 3, 4 });
                            Interlocked.Increment(ref enqueueCount);
                            Thread.Sleep(rnd.Next(0, 100));
                            session.Flush();
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Producer thread exception: {ex.Message}");
                }
            })
            { IsBackground = true, Name = "Enqueueing Thread" }; ;

            var consumerThread = new Thread(()=>
            {
                try
                {
                    do
                    {
                        using (var session = q.OpenSession())
                        {
                            var obj = session.Dequeue();
                            if (obj != null)
                            {
                                Interlocked.Increment(ref dequeueCount);
                                session.Flush();
                            }
                            else
                            {
                                //Console.WriteLine("got nothing, I'm starved for objects so backing off..");
                                Thread.Sleep(rnd.Next(0, 100));
                            }
                        }
                    }
                    while (dequeueCount < TargetObjectCount);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Consumer thread exception: {ex.Message}");
                }
            })
            { IsBackground = true, Name = "Dequeueing Thread" }; ;

            producerThread.Start();
            consumerThread.Start();

            producerThread.Join();
            consumerThread.Join();
        }

        [Benchmark]
        public async Task AsyncEnqueueDequeueConcurrentlyWithThreads()
        {
            const int TargetObjectCount = CountOfObjectsToEnqueue;
            int enqueueCount = 0, dequeueCount = 0;
            var rnd = new Random();

            IPersistentQueue q = await _factory.CreateAsync(QueuePathForAsyncThreads);

            var producerThread = new Thread(() =>
            {
                RunAsyncInThread(async () =>
                {
                    try
                    {
                        for (int i = 0; i < TargetObjectCount; i++)
                        {
                            await using (var session = await q.OpenSessionAsync())
                            {
                                await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                                Interlocked.Increment(ref enqueueCount);
                                await Task.Delay(rnd.Next(0, 100));
                                await session.FlushAsync();
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Producer thread exception: {ex.Message}");
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
                        do
                        {
                            await using (var session = await q.OpenSessionAsync())
                            {
                                var obj = await session.DequeueAsync();
                                if (obj != null)
                                {
                                    Interlocked.Increment(ref dequeueCount);
                                    await session.FlushAsync();
                                }
                                else
                                {
                                    await Task.Delay(rnd.Next(0, 100));
                                }
                            }
                        }
                        while (dequeueCount < TargetObjectCount);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Consumer thread exception: {ex.Message}");
                    }
                });
            })
            { IsBackground = true, Name = "Dequeueing Thread" };

            producerThread.Start();
            consumerThread.Start();

            producerThread.Join();
            consumerThread.Join();

            await q.DisposeAsync();
        }

        [Benchmark]
        public async Task AsyncEnqueueDequeueConcurrentlyWithTasks()
        {
            const int TargetObjectCount = CountOfObjectsToEnqueue;
            Exception? producerException = null;
            Exception? consumerException = null;

            int enqueueCount = 0;
            int dequeueCount = 0;
            var enqueueCompletionSource = new TaskCompletionSource<bool>();
            var dequeueCompletionSource = new TaskCompletionSource<bool>();

            IPersistentQueue q = await _factory.CreateAsync(QueuePathForAsyncTasks);

            // Producer task
            var producerTask = Task.Run(async () =>
            {
                try
                {
                    var rnd = new Random();
                    for (int i = 0; i < TargetObjectCount; i++)
                    {
                        await using (var session = await q.OpenSessionAsync())
                        {
                            await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                            Interlocked.Increment(ref enqueueCount);
                            await Task.Delay(rnd.Next(0, 100));
                            await session.FlushAsync();
                        }
                    }
                    enqueueCompletionSource.SetResult(true);
                }
                catch (Exception ex)
                {
                    producerException = ex;
                    enqueueCompletionSource.SetException(ex);
                }
            });

            // Consumer task
            var consumerTask = Task.Run(async () =>
            {
                try
                {
                    var rnd = new Random();
                    do
                    {
                        await using (var session = await q.OpenSessionAsync())
                        {
                            var obj = await session.DequeueAsync();
                            if (obj != null)
                            {
                                Interlocked.Increment(ref dequeueCount);
                                await session.FlushAsync();
                            }
                            else
                            {
                                //Console.WriteLine("got nothing, I'm starved for objects so backing off..");
                                await Task.Delay(rnd.Next(0, 100)); // Wait a bit if nothing to dequeue
                            }
                        }
                    }
                    while (dequeueCount < TargetObjectCount);
                    dequeueCompletionSource.SetResult(true);
                }
                catch (Exception ex)
                {
                    consumerException = ex;
                    dequeueCompletionSource.SetException(ex);
                }
            });

            // Wait for both tasks with timeout
            var completionTasks = new[]
            {
                enqueueCompletionSource.Task.WaitAsync(TimeSpan.FromMinutes(2)),
                dequeueCompletionSource.Task.WaitAsync(TimeSpan.FromMinutes(2))
            };

            try
            {
                await Task.WhenAll(completionTasks);
            }
            catch (TimeoutException)
            {
                if (!enqueueCompletionSource.Task.IsCompleted)
                    Console.WriteLine("Producer task timed out.");
                if (!dequeueCompletionSource.Task.IsCompleted)
                    Console.WriteLine("Consumer task timed out.");
            }

            await q.DisposeAsync();
        }

        private void RunAsyncInThread(Func<Task> asyncFunc)
        {
            var t = asyncFunc();
            t.GetAwaiter().GetResult();
        }
    }
}
