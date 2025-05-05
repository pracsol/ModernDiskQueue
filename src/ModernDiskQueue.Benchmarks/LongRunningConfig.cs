namespace ModernDiskQueue.Benchmarks
{
    using BenchmarkDotNet.Columns;
    using BenchmarkDotNet.Configs;
    using BenchmarkDotNet.Diagnosers;
    using BenchmarkDotNet.Exporters;
    using BenchmarkDotNet.Exporters.Csv;
    using BenchmarkDotNet.Jobs;
    using BenchmarkDotNet.Loggers;

    /// <summary>
    /// This configuration is meant for long running benchmarks, basically reducing number of warmups and iterations.
    /// </summary>
    internal class LongRunningConfig : ManualConfig
    {
        public LongRunningConfig()
        {
            AddJob(Job.Default
                .WithWarmupCount(2)
                .WithIterationCount(3)
                .WithLaunchCount(1)
                .WithId("LongRunningConfig"));

            AddLogger(ConsoleLogger.Default);
            AddColumnProvider(DefaultColumnProviders.Instance);
            AddDiagnoser(MemoryDiagnoser.Default);
            AddDiagnoser(ThreadingDiagnoser.Default);
            //AddDiagnoser(new ConcurrencyVisualizerProfiler());
            AddExporter(MarkdownExporter.Default);
            AddExporter(CsvExporter.Default);
        }
    }
}