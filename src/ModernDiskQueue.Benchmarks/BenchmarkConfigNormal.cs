// -----------------------------------------------------------------------
// <copyright file="BenchmarkConfigNormal.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Benchmarks
{
    using BenchmarkDotNet.Columns;
    using BenchmarkDotNet.Configs;
    using BenchmarkDotNet.Diagnosers;
    using BenchmarkDotNet.Exporters;
    using BenchmarkDotNet.Exporters.Csv;
    using BenchmarkDotNet.Jobs;
    using BenchmarkDotNet.Loggers;

    internal class BenchmarkConfigNormal : ManualConfig
    {
        public BenchmarkConfigNormal()
        {
            AddJob(Job.Default
                .WithWarmupCount(1)
                .WithIterationCount(1)
                .WithLaunchCount(1)
                .WithId("Baseline"));

            AddLogger(ConsoleLogger.Default);
            AddColumnProvider(DefaultColumnProviders.Instance);
            AddDiagnoser(MemoryDiagnoser.Default);
            AddDiagnoser(ThreadingDiagnoser.Default);

            // AddDiagnoser(new ConcurrencyVisualizerProfiler());
            AddExporter(MarkdownExporter.Default);
            AddExporter(CsvExporter.Default);
        }
    }
}
