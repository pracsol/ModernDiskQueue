// -----------------------------------------------------------------------
// <copyright file="FileSizeDiagnoser.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Benchmarks.CustomDiagnosers
{
    using System.Text;
    using System.Text.Json;
    using BenchmarkDotNet.Analysers;
    using BenchmarkDotNet.Diagnosers;
    using BenchmarkDotNet.Engines;
    using BenchmarkDotNet.Exporters;
    using BenchmarkDotNet.Loggers;
    using BenchmarkDotNet.Reports;
    using BenchmarkDotNet.Running;
    using BenchmarkDotNet.Validators;
    using ModernDiskQueue.Benchmarks.Helpers;

    /// <inheritdoc/>
    public class FileSizeDiagnoser : IDiagnoser
    {
        /// <summary>
        /// List to contain parsed data retrieved from the benchmark iterations.
        /// </summary>
        private readonly List<(string StrategyName, string TypeName, int IterationCount, long FileSize)> _fileSizeData = [];

        /// <inheritdoc/>
        public IEnumerable<string> Ids => [nameof(FileSizeDiagnoser)];

        /// <inheritdoc/>
        public IEnumerable<IAnalyser> Analysers => [];

        /// <inheritdoc/>
        public IEnumerable<IExporter> Exporters => [];

        /// <inheritdoc/>
        public RunMode GetRunMode(BenchmarkCase benchmarkCase) => RunMode.NoOverhead;

        /// <inheritdoc/>
        public void DisplayResults(ILogger logger)
        {
            StringBuilder rowBuilder = new();
            const int Col1Width = 31;
            const int Col2Width = 20;
            const int Col3Width = 16;
            const int Col4Width = 15;
            const string col1Name = "Strategy Name";
            const string col2Name = "Average File Size";
            const string col3Name = "Min File Size";
            const string col4Name = "Max File Size";

            rowBuilder.Append(col1Name.PadRight(Col1Width));
            rowBuilder.Append("| " + col2Name.PadRight(Col2Width));
            rowBuilder.Append("| " + col3Name.PadRight(Col3Width));
            rowBuilder.Append("| " + col4Name.PadRight(Col4Width));

            logger.WriteLine();
            logger.WriteLine("=== File Size Statistics ===");

            var statsByStrategy = _fileSizeData
                .GroupBy(t => t.StrategyName)
                .Select(g => new
                {
                    StrategyName = g.Key,
                    Average = g.Average(x => x.FileSize),
                    Min = g.Min(x => x.FileSize),
                    Max = g.Max(x => x.FileSize),
                })
                .ToList();

            logger.WriteLine(rowBuilder.ToString());

            if (statsByStrategy.Count == 0)
            {
                logger.WriteLine("No data available.");
                return;
            }
            else
            {
                foreach (var stat in statsByStrategy)
                {
                    rowBuilder.Clear();
                    rowBuilder.Append(stat.StrategyName.PadRight(Col1Width));
                    rowBuilder.Append("| " + stat.Average.ToString("N0").PadRight(Col2Width));
                    rowBuilder.Append("| " + stat.Min.ToString("N0").PadRight(Col3Width));
                    rowBuilder.Append("| " + stat.Max.ToString("N0").PadRight(Col4Width));
                    logger.WriteLine(rowBuilder.ToString());
                }
            }
        }

        /// <inheritdoc/>
        public void Handle(HostSignal signal, DiagnoserActionParameters parameters)
        {
            switch (signal)
            {
                case HostSignal.BeforeActualRun:
                    break;
                case HostSignal.AfterActualRun:
                    AfterActualRun(parameters);
                    break;
                case HostSignal.AfterAll:
                    break;
                case HostSignal.AfterProcessExit:
                    break;
            }

            // This is a fallback implementation that ensures we collect data
            // even if the direct method (AfterActualRun) isn't called
            // We can leave it empty since AfterActualRun is handling our logic
        }

        /// <inheritdoc/>
        public IEnumerable<Metric> ProcessResults(DiagnoserResults results)
        {
            //_fileSizeData.Clear();
            List<SerializerBenchmarkResult> resultList = BenchmarkDataRecorder.GetBenchmarkResults<SerializerBenchmarkResult>($"{AppContext.BaseDirectory}\\BenchmarkDotNet.Artifacts\\", false).GetAwaiter().GetResult();
            Console.WriteLine($"PR Retrieved {resultList.Count} results from the queue.");
            foreach (var result in resultList)
            {
                _fileSizeData.Add((result.Name, result.TypeName, result.IterationCount, result.FileSize));
            }

            Console.WriteLine("ValueInfo: " + results.BenchmarkCase.Parameters.ValueInfo);
            Console.WriteLine("DisplayInfo: " + results.BenchmarkCase.Parameters.DisplayInfo);
            var statsByStrategy = _fileSizeData
                .GroupBy(t => t.StrategyName)
                .Select(g => new
                {
                    StrategyName = g.Key,
                    Average = g.Average(x => x.FileSize),
                    Min = g.Min(x => x.FileSize),
                    Max = g.Max(x => x.FileSize),
                })
                .ToList();

            foreach (var stat in statsByStrategy)
            {
                yield return new Metric(new FileSizeAvgMetricDescriptor(stat.StrategyName), stat.Average);
                //yield return new Metric(new FileSizeMinMetricDescriptor(stat.StrategyName), stat.Min);
                //yield return new Metric(new FileSizeMaxMetricDescriptor(stat.StrategyName), stat.Max);
            }
        }

        /// <summary>
        /// This method is called after the actual run of the benchmark.
        /// </summary>
        /// <param name="parameters"><see cref="DiagnoserActionParameters"/>.</param>
        public void AfterActualRun(DiagnoserActionParameters parameters)
        {
            try
            {
                List<SerializerBenchmarkResult> resultList = BenchmarkDataRecorder.GetBenchmarkResults<SerializerBenchmarkResult>($"{AppContext.BaseDirectory}\\BenchmarkDotNet.Artifacts\\").GetAwaiter().GetResult();
                Console.WriteLine($"AAR Retrieved {resultList.Count} results from the queue.");
                foreach (var result in resultList)
                {
                    _fileSizeData.Add((result.Name, result.Name, result.IterationCount, result.FileSize));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing benchmark results: {ex.Message}");
            }

            Console.WriteLine($"File size data collected: {_fileSizeData.Count} entries.");
        }

        /// <inheritdoc/>
        public IEnumerable<ValidationError> Validate(ValidationParameters validationParameters) => [];
    }
}