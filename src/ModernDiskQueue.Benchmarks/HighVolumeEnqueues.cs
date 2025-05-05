namespace ModernDiskQueue.Benchmarks
{
    using System;
    using System.Threading.Tasks;
    using BenchmarkDotNet.Attributes;
    using Microsoft.Extensions.Logging;
    using ModernDiskQueue;

    [Config(typeof(CustomConfig))]
    public class HighVolumeEnqueues
    {
        private PersistentQueueFactory  _factory;
        private const string QueuePath = "AsyncEnqueue";
        public event Action<int>? ProgressUpdated;
        private static int _progressCounter = 0;

        [Params(100, 1000, 1000000)] // Will run benchmarks with 10, 100, and 1000
        public int ItemCount;

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
        public async Task AsyncEnqueueItemsWithSingleFlush()
        {
            int countOfItemsToEnqueue = ItemCount;
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
        public void SyncEnqueueItemsWithSingleFlush()
        {
            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    for (int i = 0; i < ItemCount; i++)
                    {
                        session.Enqueue(Guid.NewGuid().ToByteArray());
                    }
                    session.Flush();
                }
            }
        }
    }
}
