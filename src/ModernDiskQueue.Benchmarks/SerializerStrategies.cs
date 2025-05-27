// -----------------------------------------------------------------------
// <copyright file="SerializerStrategies.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Benchmarks
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading.Tasks;
    using BenchmarkDotNet.Attributes;
    using BenchmarkDotNet.Columns;
    using BenchmarkDotNet.Configs;
    using BenchmarkDotNet.Diagnosers;
    using BenchmarkDotNet.Exporters;
    using BenchmarkDotNet.Jobs;
    using BenchmarkDotNet.Loggers;
    using Microsoft.Extensions.Logging;
    using ModernDiskQueue.Benchmarks.CustomDiagnosers;
    using ModernDiskQueue.Benchmarks.CustomSerializers;
    using ModernDiskQueue.Benchmarks.Helpers;
    using ModernDiskQueue.Benchmarks.SampleData;
    using ModernDiskQueue.Implementation;

    /// <summary>
    /// Provides a set of strategies and configurations for benchmarking serialization methods.
    /// </summary>
    /// <remarks>This class is designed to facilitate benchmarking of various serialization strategies,
    /// including XML, JSON, and MessagePack. It supports configuration of parameters, iteration setup, cleanup, and
    /// execution of serialization tests. The results of the benchmarks, such as file sizes and performance metrics, can
    /// be recorded for analysis.</remarks>
    [Config(typeof(Config))]
    public partial class SerializerStrategies
    {
        /// <summary>
        /// Number of objects to enqueue for each iteration. If testing for size, this doesn't
        /// really matter, but if testing for speed, you may try increasing the number >10 to
        /// get statistically relevant results.
        /// </summary>
        private const int CountOfObjectsToEnqueue = 3;
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
        /// Counter to track the number of iterations that have been run.
        /// </summary>
        private int _iterationCounter = 0;

        /// <summary>
        /// Counter to track the number of actual workload iterations that have been run.
        /// </summary>
        private int _actualWorkloadIterationCounter = 0;

        /// <summary>
        /// Reflects whether the current iteration is an actual workload.
        /// </summary>
        private bool _isActualWorkload = false;

        // Add the following property to provide the list of serialization strategies
        public static IEnumerable<SerializerBenchmarkCase> SerializerCases =>
        [
            new SerializerBenchmarkCase("XML (built-in)", new SerializationStrategyXml<SampleDataObject>(), "q_SerializerStrategies_Xml"),
            new SerializerBenchmarkCase("JSON (built-in)", new SerializationStrategyJson<SampleDataObject>(), "q_SerializerStrategies_Json"),
            new SerializerBenchmarkCase("MessagePack (custom)", new SerializationStrategyMsgPack<SampleDataObject>(), "q_SerializerStrategies_MsgPack"),
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
            const int NumberOfJittingIterations = 1;

            // increment the iteration counter
            _iterationCounter++;

            Console.WriteLine($"// Setup on iteration {_iterationCounter} of Case: {Case.Name}");

            // need to determine whether the event is fired from jitting, warmups, or iterations.
            // Jitting will happen once?
            // Warmups will happen per Config.WarmupCount
            // Each "case" in the params will be run as a separate process, so the counters reset.
            _actualWorkloadIterationCounter = _iterationCounter - (NumberOfJittingIterations + Config.WarmupCount);

            _isActualWorkload = _actualWorkloadIterationCounter > 0 && _actualWorkloadIterationCounter <= Config.IterationCount;

            Console.WriteLine($"// Is this an actual workload? " + (_isActualWorkload ? $"Yes, number {_actualWorkloadIterationCounter}." : "No."));

            Console.WriteLine("// Running...");

            // delete any queue artifacts that may exist
            // FileManagement.AttemptManualCleanup(Case.QueuePath + (_iterationCounter -1));
        }

        [IterationCleanup]
        public void IterationCleanup()
        {
            string queuePath = Case.QueuePath + _iterationCounter;

            long fileSizeInBytes = 0;

            Console.WriteLine($"// Cleanup on iteration {_iterationCounter}");


            if (_isActualWorkload)
            {
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
                FileSizeResult result = new()
                {
                    GroupName = Case.Name,
                    IterationCount = _actualWorkloadIterationCounter,
                    FileSize = fileSizeInBytes,
                };

                BenchmarkDataRecorder.SaveBenchmarkResult<FileSizeResult>(result).GetAwaiter().GetResult();
            }
            else
            {
                Console.WriteLine("// Skipping file size data write because this isn't a valid workload.");
            }
        }

        /// <summary>
        /// Test the serializer speed and size.
        /// </summary>
        /// <remarks>
        /// Need to be really careful about only executing single op per iteration.
        /// If testing speed, include a great number of iterations and increase number of objects
        /// to enqueue to generate statisticaly relevant results that have lower stdev. Since file
        /// size should be constant across iterations (as long as they don't time out) then doing
        /// only one iteration or a few to be safe should tell you the size results, but speed
        /// requires a more intense test.
        /// </remarks>
        /// <returns>Return value is only included to prevent dead code elimination.</returns>
        [Benchmark(OperationsPerInvoke = 1)]
        public async Task<int> TestSerializerSpeedAndSize()
        {
            int targetObjectCount = CountOfObjectsToEnqueue;
            ISerializationStrategy<SampleDataObject> serializer = Case.Strategy;
            string queuePath = Case.QueuePath + _iterationCounter;
            Exception? producerException = null;
            Exception? consumerException = null;

            int enqueueCount = 0;
            int dequeueCount = 0;
            var enqueueCompletionSource = new TaskCompletionSource<bool>();
            var dequeueCompletionSource = new TaskCompletionSource<bool>();

            IPersistentQueue<SampleDataObject> q = await _factory.CreateAsync<SampleDataObject>(queuePath);

            // Console.WriteLine($"Creating task to enqueue {targetObjectCount} objects");
            // Producer task
            var producerTask = Task.Run(async () =>
            {
                try
                {
                    var rnd = new Random();
                    for (int i = 0; i < targetObjectCount; i++)
                    {
                        await using (var session = await q.OpenSessionAsync(serializer, CancellationToken.None))
                        {
                            await session.EnqueueAsync(_arrayOfSamples[i]);
                            Interlocked.Increment(ref enqueueCount);
                            await Task.Delay(rnd.Next(0, 100));
                            await session.FlushAsync();
                        }
                    }

                    // Console.WriteLine($"Enqueued {enqueueCount} objects.");
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
                    while (dequeueCount < targetObjectCount);

                    // Console.WriteLine($"Dequeued {dequeueCount} objects");
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
                // Console.WriteLine("Starting tasks..");
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

            // Console.WriteLine("disposing q");
            await q.DisposeAsync();
            return enqueueCount;
        }

        internal class Config : ManualConfig
        {
            public const int WarmupCount = 2;
            public const int IterationCount = 20;
            public const int LaunchCount = 1;
            public const int UnrollFactor = 1;

            public Config()
            {
                AddJob(Job.Default
                    .WithWarmupCount(WarmupCount)
                    .WithIterationCount(IterationCount)
                    .WithInvocationCount(1)
                    .WithLaunchCount(LaunchCount)
                    .WithUnrollFactor(UnrollFactor)
                    .WithId("SerializerStrategies"));

                // .WithOptions(ConfigOptions.DisableOptimizationsValidator)
                AddLogger(ConsoleLogger.Default);
                AddColumnProvider(DefaultColumnProviders.Instance);
                AddDiagnoser(ThreadingDiagnoser.Default);
                AddDiagnoser(MemoryDiagnoser.Default);
                AddDiagnoser(new FileSizeDiagnoser());

                // AddDiagnoser(new ConcurrencyVisualizerProfiler());
                AddExporter(MarkdownExporter.Default);

                // AddExporter(CsvExporter.Default);
            }
        }
    }
}