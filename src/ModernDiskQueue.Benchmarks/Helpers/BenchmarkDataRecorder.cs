namespace ModernDiskQueue.Benchmarks.Helpers
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
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
        /// <param name="dataLine">string representation of data to enqueue, e.g. comma-delimited list of values.</param>
        /// <returns>Nothing.</returns>
        public static async Task SaveBenchmarkResult(string dataLine)
        {
            if (!string.IsNullOrEmpty(_resolvedArtifactsPath) && !Directory.Exists(_resolvedArtifactsPath))
            {
                Directory.CreateDirectory(_resolvedArtifactsPath);
            }

            Console.WriteLine($"Attempting to enqueue at {_resolvedArtifactsPath}{_queueName}");
            await using (var q = await _queueFactory.WaitForAsync<string>($"{_pathToArtifactsFolder}{_queueName}", TimeSpan.FromSeconds(2)))
            {
                await using (var s = await q.OpenSessionAsync())
                {
                    await s.EnqueueAsync(dataLine);
                    await s.FlushAsync();
                }
            }
        }

        /// <summary>
        /// Reads strings from queue and returns collection as a List.
        /// </summary>
        /// <returns>List of values.</returns>
        public static async Task<List<string>> GetBenchmarkResults()
        {
            return await GetBenchmarkResults(_resolvedArtifactsPath);
        }

        public static async Task<List<string>> GetBenchmarkResults(string pathToArtifactsFolder)
        {
            List<string> results = [];

            await using (var q = await _queueFactory.WaitForAsync<string>($"{pathToArtifactsFolder}{_queueName}", TimeSpan.FromSeconds(5)))
            {
                await using (var s = await q.OpenSessionAsync())
                {
                    string dataFromQueue = string.Empty;
                    do
                    {
                        dataFromQueue = await s.DequeueAsync();
                        if (dataFromQueue != null && dataFromQueue != string.Empty)
                        {
                            Console.WriteLine($"Dequeued data: {dataFromQueue}");
                            results.Add(dataFromQueue);
                            await s.FlushAsync();
                        }
                    }
                    while (dataFromQueue != null && dataFromQueue != string.Empty);
                }
            }

            Console.WriteLine($"Returning {results.Count} results from queue.");
            return results;
        }
    }
}