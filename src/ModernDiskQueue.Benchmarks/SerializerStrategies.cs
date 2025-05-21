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
    using ModernDiskQueue.Benchmarks.CustomDiagnosers;
    using ModernDiskQueue.Benchmarks.Helpers;
    using ModernDiskQueue.Benchmarks.SampleData;
    using ModernDiskQueue.Implementation;

    [Config(typeof(Config))]
    public partial class SerializerStrategies
    {
        private const int CountOfObjectsToEnqueue = 10;
        private readonly SampleDataObject[] _arrayOfSamples = new SampleDataObject[CountOfObjectsToEnqueue];

        /// <summary>
        /// Factory to create persistent queues.
        /// </summary>
        /// <remarks>
        /// I had a lot of difficulty getting data from the benchmark post iteration events (collecting size of the Data.0 file) back up to the benchmark process.
        /// I had explored in-memory solutions and writing out to a CSV. I had seen some discussion online about using SqlLite for this, which makes sense, but
        /// why not just eat my own dog food and use MDQ? I already have the library referenced in the benchmark project, which would avoid the need to add another dependency.
        /// So that's why it's here: each IterationCleanup method will write out the file size of the Data.0 file to a queue, which will remain available to be
        /// read by the diagnoser running in the main benchmark process.
        /// </remarks>
        private PersistentQueueFactory _factory = new();

        /// <summary>
        /// Counter to track the number of times the IterationSetup method has been called.
        /// </summary>
        private int _setupCounter = 0;

        /// <summary>
        /// Counter to track the number of times the IterationCleanup method has been called.
        /// </summary>
        private int _cleanupCounter = 0;

        // Add the following property to provide the list of serialization strategies
        public static IEnumerable<SerializerBenchmarkCase> SerializerCases =>
        [
            new SerializerBenchmarkCase("XML", new SerializationStrategyXml<SampleDataObject>(), "q_SerializerStrategies_Xml"),
            new SerializerBenchmarkCase("JSON", new SerializationStrategyJson<SampleDataObject>(), "q_SerializerStrategies_Json"),
        ];

        /// <summary>
        /// Gets or sets the current parameter to use with the benchmark.
        /// </summary>
        [ParamsSource(nameof(SerializerCases), Priority = -100)]
        public SerializerBenchmarkCase Case { get; set; } = default!;

        [GlobalSetup]
        /// <inheritdoc/>
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
                _arrayOfSamples[x] = SampleDataFactory.CreateRandomSampleData();
            }
        }

        [IterationSetup]
        public void IterationSetup()
        {
            int currentIteration = ++_setupCounter;

            Console.WriteLine($"// Iteration Setup on iteration {currentIteration}");
            FileManagement.AttemptManualCleanup(Case.QueuePath);
        }

        [IterationCleanup()]
        public void IterationCleanup()
        {
            const int NumberOfJittingIterations = 1;
            string queuePath = Case.QueuePath;

            long fileSizeInBytes = 0;

            int currentIteration = ++_cleanupCounter;

            Console.WriteLine($"// Iteration Cleanup on iteration {currentIteration}");

            // need to determine whether the event is fired from jitting, warmups, or iterations.
            // Jitting will happen twice?
            // Warmups will happen per Config.WarmupCount
            // Each "case" in the params will be run as a separate process, so the counters reset.
            currentIteration = currentIteration - (NumberOfJittingIterations + Config.WarmupCount);

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
                SerializerBenchmarkResult result = new()
                {
                    Name = Case.Name,
                    TypeName = Case.Strategy.GetType().Name,
                    IterationCount = currentIteration,
                    FileSize = fileSizeInBytes,
                };

                string rowData = $"{Case.Name}, {Case.Strategy.GetType().Name}, {currentIteration}, {fileSizeInBytes}";
                BenchmarkDataRecorder.SaveBenchmarkResult<SerializerBenchmarkResult>(result).GetAwaiter().GetResult();
            }
            else
            {
                Console.WriteLine("// Skipping file size data write because this isn't a valid workload.");
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
                        await using (var session = await q.OpenSessionAsync(serializer, CancellationToken.None))
                        {
                            await session.EnqueueAsync(_arrayOfSamples[i]);
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
            public const int IterationCount = 2;
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