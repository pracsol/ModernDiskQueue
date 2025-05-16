// -----------------------------------------------------------------------
// <copyright file="BenchmarkConfigThreadTaskComparison.cs" company="ModernDiskQueue Contributors">
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

    /// <summary>
    /// This configuration is meant for long running benchmarks, basically reducing number of warmups and iterations.
    /// </summary>
    internal class BenchmarkConfigThreadTaskComparison : ManualConfig
    {
        public BenchmarkConfigThreadTaskComparison()
        {
            AddJob(Job.Default
                .WithWarmupCount(2)
                .WithIterationCount(20)
                .WithLaunchCount(2)
                .WithId("BenchmarkConfigThreadTaskComparison"));

            AddLogger(ConsoleLogger.Default);
            AddColumnProvider(DefaultColumnProviders.Instance);
            AddDiagnoser(ThreadingDiagnoser.Default);
            AddDiagnoser(MemoryDiagnoser.Default);

            // AddDiagnoser(new ConcurrencyVisualizerProfiler());
            AddExporter(MarkdownExporter.Default);
            AddExporter(CsvExporter.Default);
        }
    }
}