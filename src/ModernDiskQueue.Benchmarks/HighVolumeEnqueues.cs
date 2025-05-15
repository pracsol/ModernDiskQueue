namespace ModernDiskQueue.Benchmarks
{
    using System;
    using System.Threading.Tasks;
    using BenchmarkDotNet.Attributes;
    using Microsoft.Extensions.Logging;
    using ModernDiskQueue;

    [Config(typeof(BenchmarkConfigNormal))]
    public class HighVolumeEnqueues
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
        public async Task AsyncEnqueueMillionItemsWithSingleFlush()
        {
            int countOfItemsToEnqueue = 1000000;
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int i = 0; i < countOfItemsToEnqueue; i++)
                    {
                        await session.EnqueueAsync(Guid.NewGuid().ToByteArray());
                    }
                    await session.FlushAsync();
                }
            }
        }

        [Benchmark]
        public void SyncEnqueueMillionItemsWithSingleFlush()
        {
            int countOfItemsToEnqueue = 1000000;
            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    for (int i = 0; i < countOfItemsToEnqueue; i++)
                    {
                        session.Enqueue(Guid.NewGuid().ToByteArray());
                    }
                    session.Flush();
                }
            }
        }

        [Benchmark]
        public async Task AsyncEnqueueAndDequeueItemsWithBigFlush()
        {
            int countOfItemsToEnqueue = 1000;
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int i = 0; i < countOfItemsToEnqueue; i++)
                    {
                        await session.EnqueueAsync(Guid.NewGuid().ToByteArray());
                    }
                    await session.FlushAsync();
                }
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int i = 0; i < countOfItemsToEnqueue; i++)
                    {
                        var data = await session.DequeueAsync();
                        if (data == null)
                        {
                            throw new Exception("Dequeue failed");
                        }
                    }
                    await session.FlushAsync();
                }
            }
        }

        [Benchmark]
        public void SyncEnqueueAndDequeueItemsWithBigFlush()
        {
            int countOfItemsToEnqueue = 1000;
            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    for (int i = 0; i < countOfItemsToEnqueue; i++)
                    {
                        session.Enqueue(Guid.NewGuid().ToByteArray());
                    }
                    session.Flush();
                }
            }
            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    for (int i = 0; i < countOfItemsToEnqueue; i++)
                    {
                        var data = session.Dequeue();
                        if (data == null)
                        {
                            throw new Exception("Dequeue failed");
                        }
                    }
                    session.Flush();
                }
            }
        }

        [Benchmark]
        public async Task AsyncEnqueueAndDequeueItemsWithCourtesyFlush()
        {
            int countOfItemsToEnqueue = 1000;
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int i = 0; i < countOfItemsToEnqueue; i++)
                    {
                        await session.EnqueueAsync(Guid.NewGuid().ToByteArray());
                        await session.FlushAsync();
                    }
                }
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int i = 0; i < countOfItemsToEnqueue; i++)
                    {
                        var data = await session.DequeueAsync();
                        if (data == null)
                        {
                            throw new Exception("Dequeue failed");
                        }
                        await session.FlushAsync();
                    }
                }
            }
        }

        [Benchmark]
        public void SyncEnqueueAndDequeueItemsWithCourtesyFlush()
        {
            int countOfItemsToEnqueue = 1000;
            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    for (int i = 0; i < countOfItemsToEnqueue; i++)
                    {
                        session.Enqueue(Guid.NewGuid().ToByteArray());
                        session.Flush();
                    }
                }
            }
            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    for (int i = 0; i < countOfItemsToEnqueue; i++)
                    {
                        var data = session.Dequeue();
                        if (data == null)
                        {
                            throw new Exception("Dequeue failed");
                        }
                        session.Flush();
                    }
                }
            }
        }
    }
}
