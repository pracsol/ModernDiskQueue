// -----------------------------------------------------------------------
// <copyright file="SerializerStrategies.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Benchmarks
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using BenchmarkDotNet.Attributes;
    using Microsoft.Extensions.Logging;
    using ModernDiskQueue.Benchmarks.SampleData;
    using ModernDiskQueue.Implementation;

    [Config(typeof(BenchmarkConfigThreadTaskComparison))]
    public class SerializerStrategies
    {
        private const int CountOfObjectsToEnqueue = 10;
        private const string QueuePath = "q_SerializerStrategies";
        private PersistentQueueFactory _factory = new ();
        private readonly SampleDataObject[] arrayOfSamples = new SampleDataObject[CountOfObjectsToEnqueue];

        // Add the following property to provide the list of serialization strategies
        public static IEnumerable<ISerializationStrategy<SampleDataObject>> SerializationStrategies => new ISerializationStrategy<SampleDataObject>[]
        {
            new SerializationStrategyXml<SampleDataObject>(),
            new SerializationStrategyJson<SampleDataObject>(),
        };

        [ParamsSource(nameof(SerializationStrategies))]
        public ISerializationStrategy<SampleDataObject> _serializer;

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
            for (int x = 0; x < CountOfObjectsToEnqueue; x++)
            {
                arrayOfSamples[x] = SampleDataFactory.CreateRandomSampleData();
            }
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
        public async Task AsyncEnqueueDequeueConcurrentlyWithTasks()
        {
            const int TargetObjectCount = CountOfObjectsToEnqueue;
            Exception? producerException = null;
            Exception? consumerException = null;

            int enqueueCount = 0;
            int dequeueCount = 0;
            var enqueueCompletionSource = new TaskCompletionSource<bool>();
            var dequeueCompletionSource = new TaskCompletionSource<bool>();

            IPersistentQueue<SampleDataObject> q = await _factory.CreateAsync<SampleDataObject>(QueuePath);

            // Producer task
            var producerTask = Task.Run(async () =>
            {
                try
                {
                    var rnd = new Random();
                    for (int i = 0; i < TargetObjectCount; i++)
                    {
                        await using (var session = await q.OpenSessionAsync(_serializer))
                        {
                            await session.EnqueueAsync(arrayOfSamples[i]);
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
                        await using (var session = await q.OpenSessionAsync(_serializer))
                        {
                            var obj = await session.DequeueAsync();
                            if (obj != null)
                            {
                                Interlocked.Increment(ref dequeueCount);
                                await session.FlushAsync();
                            }
                            else
                            {
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
                dequeueCompletionSource.Task.WaitAsync(TimeSpan.FromMinutes(2)),
            };

            try
            {
                await Task.WhenAll(completionTasks);
            }
            catch (TimeoutException)
            {
                if (!enqueueCompletionSource.Task.IsCompleted)
                {
                    Console.WriteLine("Producer task timed out.");
                }

                if (!dequeueCompletionSource.Task.IsCompleted)
                {
                    Console.WriteLine("Consumer task timed out.");
                }
            }

            await q.DisposeAsync();
        }
    }
}
