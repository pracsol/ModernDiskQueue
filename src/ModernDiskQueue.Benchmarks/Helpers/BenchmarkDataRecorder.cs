// -----------------------------------------------------------------------
// <copyright file="BenchmarkDataRecorder.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Benchmarks.Helpers
{
    using System;
    using System.Collections.Generic;
    using System.Text.Json;
    using System.Threading.Tasks;
    using ModernDiskQueue;

    public static class BenchmarkDataRecorder
    {
        private static string _queueName = "benchmarkresultsqueue";
        private static string _pathToArtifactsFolder = $"{AppContext.BaseDirectory}\\..\\..\\..\\..\\BenchmarkDotNet.Artifacts\\";
        private static string _resolvedArtifactsPath;
        private static PersistentQueueFactory _queueFactory;

        static BenchmarkDataRecorder()
        {
            _queueFactory = new PersistentQueueFactory();

            // This is process specific, so it's valid for the child process spun up by benchmarkdotnet, but when the diagnoser runs it will have to pass in its own path.
            _resolvedArtifactsPath = Path.GetFullPath(_pathToArtifactsFolder);
        }

        /// <summary>
        /// Enqueues a string to a persistent queue for later processing.
        /// </summary>
        /// <typeparam name="T">Type of data to save.</typeparam>
        /// <param name="benchmarkData">Instance object of data to save.</param>
        /// <returns>Nothing.</returns>
        public static async Task SaveBenchmarkResult<T>(T benchmarkData)
        {
            ArgumentNullException.ThrowIfNull(benchmarkData, nameof(benchmarkData));

            if (!string.IsNullOrEmpty(_resolvedArtifactsPath) && !Directory.Exists(_resolvedArtifactsPath))
            {
                Directory.CreateDirectory(_resolvedArtifactsPath);
            }

            Console.WriteLine($"Attempting to enqueue at {_resolvedArtifactsPath}{_queueName}");
            await using (var q = await _queueFactory.WaitForAsync<T>($"{_pathToArtifactsFolder}{_queueName}", TimeSpan.FromSeconds(2)))
            {
                await using (var s = await q.OpenSessionAsync())
                {
                    await s.EnqueueAsync(benchmarkData);
                    await s.FlushAsync();
                }
            }
        }

        /// <summary>
        /// Reads objects from queue and returns collection as a List.
        /// </summary>
        /// <returns>List of values.</returns>
        public static async Task<List<T>> GetBenchmarkResults<T>()
        {
            return await GetBenchmarkResults<T>(_resolvedArtifactsPath);
        }

        /// <summary>
        /// Reads objects from queue and returns collection as a List.
        /// </summary>
        /// <typeparam name="T">Type of data to extract.</typeparam>
        /// <param name="pathToArtifactsFolder">File path to the queue folder.</param>
        /// <param name="destructiveRead">If false, the value will stay in the queue after it's read. If true, the value is removed from the queue (default).</param>
        /// <returns>Collection of deserialized objects.</returns>
        public static async Task<List<T>> GetBenchmarkResults<T>(string pathToArtifactsFolder, bool destructiveRead = true)
        {
            List<T> results = [];

            await using (var q = await _queueFactory.WaitForAsync<T>($"{pathToArtifactsFolder}{_queueName}", TimeSpan.FromSeconds(5)))
            {
                await using (var s = await q.OpenSessionAsync())
                {
                    object? dataFromQueue = null;
                    do
                    {
                        dataFromQueue = await s.DequeueAsync();
                        if (dataFromQueue != null)
                        {
                            Console.WriteLine($"Dequeued data: {dataFromQueue}");
                            if (dataFromQueue.GetType() != typeof(T))
                            {
                                // just swallow this and move on since maybe there's multiple types in the queue??
                                // throw new InvalidOperationException($"Data type mismatch. Expected {typeof(T)}, but got {dataFromQueue.GetType()}");
                            }
                            else
                            {
                                results.Add((T)dataFromQueue);
                                if (destructiveRead)
                                {
                                    await s.FlushAsync();
                                }
                            }
                        }
                    }
                    while (dataFromQueue != null);
                }
            }

            Console.WriteLine($"Returning {results.Count} results from queue.");
            return results;
        }
    }
}