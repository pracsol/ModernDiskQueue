namespace ModernDiskQueue.Benchmarks
{
    using BenchmarkDotNet.Diagnosers;
    using BenchmarkDotNet.Exporters.Csv;
    using BenchmarkDotNet.Exporters;
    using BenchmarkDotNet.Jobs;
    using BenchmarkDotNet.Configs;
    using BenchmarkDotNet.Loggers;
    using BenchmarkDotNet.Columns;

    internal class NormalTestConfig : ManualConfig
    {
        public NormalTestConfig()
        {
            AddJob(Job.Default
                .WithWarmupCount(2)
                .WithIterationCount(3)
                .WithLaunchCount(1)
                .WithId("Baseline"));

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
