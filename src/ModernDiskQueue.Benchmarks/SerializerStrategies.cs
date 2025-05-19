// -----------------------------------------------------------------------
// <copyright file="SerializerStrategies.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Benchmarks
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Threading.Tasks;
    using BenchmarkDotNet.Attributes;
    using BenchmarkDotNet.Columns;
    using BenchmarkDotNet.Configs;
    using BenchmarkDotNet.Diagnosers;
    using BenchmarkDotNet.Exporters;
    using BenchmarkDotNet.Exporters.Csv;
    using BenchmarkDotNet.Jobs;
    using BenchmarkDotNet.Loggers;
    using Microsoft.Extensions.Logging;
    using ModernDiskQueue.Benchmarks.Helpers;
    using ModernDiskQueue.Benchmarks.SampleData;
    using ModernDiskQueue.Implementation;

    [Config(typeof(Config))]
    public class SerializerStrategies
    {
        private const int CountOfObjectsToEnqueue = 10;
        private PersistentQueueFactory _factory = new();

        private int cleanupCounter = 1;
        private int setupCounter = 1;

        public record SerializerCase(
            ISerializationStrategy<SampleDataObject> Strategy,
            string QueuePath);

        private readonly SampleDataObject[] arrayOfSamples = new SampleDataObject[CountOfObjectsToEnqueue];

        // Static dictionary to store file sizes per benchmark case
        public static ConcurrentDictionary<string, long> FileSizes = new();


        // Add the following property to provide the list of serialization strategies
        public static IEnumerable<SerializerCase> SerializerCases =>
        [
            new SerializerCase(new SerializationStrategyXml<SampleDataObject>(), "q_SerializerStrategies_Xml"),
           // new SerializerCase(new SerializationStrategyJson<SampleDataObject>(), "q_SerializerStrategies_Json"),
        ];

        [ParamsSource(nameof(SerializerCases))]
        public SerializerCase Case { get; set; }

        [GlobalSetup]
        public void Setup()
        {
            Console.WriteLine("// " + "GlobalSetup");
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
            Console.WriteLine("// " + "GlobalCleanup");
        }

        [IterationSetup]
        public void IterationSetup()
        {
            // Setup count is not zero-based.
            int currentIteration = ++setupCounter;

            Console.WriteLine($"// Iteration Setup on iteration {currentIteration}");
            FileManagement.AttemptManualCleanup(Case.QueuePath);
        }

        [IterationCleanup()]
        public void IterationCleanup()
        {
            string queuePath = Case.QueuePath;

            long fileSizeInBytes = 0;

            // Cleanup count is not zero-based.
            int currentIteration = ++cleanupCounter;

            Console.WriteLine($"// Iteration Cleanup on iteration {currentIteration}");

            // need to determine whether the event is fired from jitting, warmups, or iterations.
            // Jitting will happen once? Warmups will happen per Config.WarmupCount
            currentIteration = currentIteration - 2 - (Config.WarmupCount * SerializerCases.Count());

            if (currentIteration > 0 && currentIteration <= Config.IterationCount)
            {
                Console.WriteLine($"// Writing file size data for workload iteration {currentIteration}");
                // Get the data file size.
                string dataFilePath = Path.Combine(queuePath, "Data.0");

                if (File.Exists(dataFilePath))
                {
                    fileSizeInBytes = new FileInfo(dataFilePath).Length;
                    Console.WriteLine($"// File size was {fileSizeInBytes} bytes.");
                }
                else
                {
                    Console.WriteLine($"// ---->> File Data.0 does not exist.");
                    fileSizeInBytes = 0;
                }

                // write to file.. serializationstrategyname, iterationcount, filesize
                string rowData = $"{Case.Strategy.GetType().Name}, {currentIteration}, {fileSizeInBytes}";
                CsvFileHelper.AppendLineToCsv(rowData);
            }
            else
            {
                Console.WriteLine("// Skipping file size data write because this isn't a valid iteration.");
            }
        }

        [Benchmark]
        public async Task TestSerializerSpeedAndSize()
        {
            const int TargetObjectCount = CountOfObjectsToEnqueue;
            ISerializationStrategy<SampleDataObject> serializer = Case.Strategy;
            string queuePath = Case.QueuePath;
            Exception? producerException = null;
            Exception? consumerException = null;

            int enqueueCount = 0;
            int dequeueCount = 0;
            var enqueueCompletionSource = new TaskCompletionSource<bool>();
            var dequeueCompletionSource = new TaskCompletionSource<bool>();

            IPersistentQueue<SampleDataObject> q = await _factory.CreateAsync<SampleDataObject>(queuePath);

            // Producer task
            var producerTask = Task.Run(async () =>
            {
                try
                {
                    var rnd = new Random();
                    for (int i = 0; i < TargetObjectCount; i++)
                    {
                        await using (var session = await q.OpenSessionAsync(serializer))
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
                        await using (var session = await q.OpenSessionAsync(serializer))
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

        internal class Config : ManualConfig
        {
            public const int WarmupCount = 2;
            public const int IterationCount = 1;
            public const int LaunchCount = 1;
            public const int UnrollFactor = 1;

            public Config()
            {
                AddJob(Job.Default
                    .WithWarmupCount(WarmupCount)
                    .WithIterationCount(IterationCount)
                    .WithInvocationCount(IterationCount)
                    .WithLaunchCount(LaunchCount)
                    .WithUnrollFactor(UnrollFactor)
                    .WithId("SerializerStrategies"));
                    // .WithOptions(ConfigOptions.DisableOptimizationsValidator);

                AddLogger(ConsoleLogger.Default);
                AddColumnProvider(DefaultColumnProviders.Instance);
                AddDiagnoser(ThreadingDiagnoser.Default);
                AddDiagnoser(MemoryDiagnoser.Default);
                AddDiagnoser(new FileSizeDiagnoser());

                // AddDiagnoser(new ConcurrencyVisualizerProfiler());
                AddExporter(MarkdownExporter.Default);
                AddExporter(CsvExporter.Default);
            }
        }
    }
}