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
    using Microsoft.CodeAnalysis.CSharp.Syntax;
    using ModernDiskQueue.Benchmarks.Helpers;

    /// <inheritdoc/>
    public class FileSizeDiagnoser : IDiagnoser
    {
        /// <summary>
        /// List to contain parsed data retrieved from the benchmark iterations.
        /// </summary>
        private readonly List<(string GroupName, int IterationCount, long FileSize)> _fileSizeData = [];

        /// <inheritdoc/>
        public IEnumerable<string> Ids => [nameof(FileSizeDiagnoser)];

        /// <inheritdoc/>
        public IEnumerable<IAnalyser> Analysers => [];

        /// <inheritdoc/>
        public IEnumerable<IExporter> Exporters => [];

        /// <inheritdoc/>
        public RunMode GetRunMode(BenchmarkCase benchmarkCase) => RunMode.NoOverhead;

        /// <summary>
        /// This method is responsible for displaying the diagnoser detailed results.
        /// </summary>
        public void DisplayResults(ILogger logger)
        {
            StringBuilder rowBuilder = new();
            const int Col1Width = 30;
            const int Col2Width = 13;
            const int Col3Width = 20;
            const int Col4Width = 16;
            const int Col5Width = 16;
            const string col1Name = "Strategy Name";
            const string col2Name = "Iter. Count";
            const string col3Name = "Average File Size";
            const string col4Name = "Min File Size";
            const string col5Name = "Max File Size";

            rowBuilder.Append($"| {col1Name}".PadRight(Col1Width));
            rowBuilder.Append($"| {col2Name}".PadRight(Col2Width));
            rowBuilder.Append($"| {col3Name}".PadRight(Col3Width));
            rowBuilder.Append($"| {col4Name}".PadRight(Col4Width));
            rowBuilder.Append($"| {col5Name}".PadRight(Col5Width) + '|');
            rowBuilder.AppendLine();
            rowBuilder.Append("|".PadRight(Col1Width, '-'));
            rowBuilder.Append("|".PadRight(Col2Width, '-'));
            rowBuilder.Append("|".PadRight(Col3Width, '-'));
            rowBuilder.Append("|".PadRight(Col4Width, '-'));
            rowBuilder.Append("|".PadRight(Col5Width, '-') + '|');

            logger.WriteLine();
            logger.WriteLine("=== File Size Statistics ===");
            logger.WriteLine(rowBuilder.ToString());

            var statsByStrategy = _fileSizeData
                .GroupBy(t => t.GroupName)
                .Select(g => new
                {
                    StrategyName = g.Key,
                    NumberOfIterations = g.Count(),
                    Average = g.Average(x => x.FileSize),
                    Min = g.Min(x => x.FileSize),
                    Max = g.Max(x => x.FileSize),
                })
                .ToList();

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
                    rowBuilder.Append($"| {stat.StrategyName}".PadRight(Col1Width));
                    rowBuilder.Append($"| {stat.NumberOfIterations}".PadRight(Col2Width));
                    rowBuilder.Append($"| {stat.Average.ToString("N0")}".PadRight(Col3Width));
                    rowBuilder.Append($"| {stat.Min.ToString("N0")}".PadRight(Col4Width));
                    rowBuilder.Append($"| {stat.Max.ToString("N0")}".PadRight(Col5Width) + '|');
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
                    AfterProcessExit(parameters);
                    break;
            }

            // This is a fallback implementation that ensures we collect data
            // even if the direct method (AfterActualRun) isn't called
            // We can leave it empty since AfterActualRun is handling our logic
        }

        /// <Summary>
        /// This method is what yields metrics to the benchmark summary.
        /// </Summary>
        public IEnumerable<Metric> ProcessResults(DiagnoserResults results)
        {
            //_fileSizeData.Clear();
            List<FileSizeResult> resultList = BenchmarkDataRecorder.GetBenchmarkResults<FileSizeResult>($"{AppContext.BaseDirectory}\\BenchmarkDotNet.Artifacts\\", false).GetAwaiter().GetResult();
            Console.WriteLine($"PR Retrieved {resultList.Count} results from the queue.");
            foreach (var result in resultList)
            {
                _fileSizeData.Add((result.GroupName, result.IterationCount, result.FileSize));
            }

            Console.WriteLine("ValueInfo: " + results.BenchmarkCase.Parameters.ValueInfo);
            Console.WriteLine("DisplayInfo: " + results.BenchmarkCase.Parameters.DisplayInfo);
            var statsByStrategy = _fileSizeData
                .GroupBy(t => t.GroupName)
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
                List<FileSizeResult> resultList = BenchmarkDataRecorder.GetBenchmarkResults<FileSizeResult>($"{AppContext.BaseDirectory}\\BenchmarkDotNet.Artifacts\\").GetAwaiter().GetResult();
                Console.WriteLine($"AAR Retrieved {resultList.Count} results from the queue.");
                foreach (var result in resultList)
                {
                    _fileSizeData.Add((result.GroupName, result.IterationCount, result.FileSize));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing benchmark results: {ex.Message}");
            }

            Console.WriteLine($"File size data collected: {_fileSizeData.Count} entries.");
        }

        public void AfterProcessExit(DiagnoserActionParameters parameters)
        {
            BenchmarkDataRecorder.CleanupSavedData($"{AppContext.BaseDirectory}\\BenchmarkDotNet.Artifacts\\").GetAwaiter().GetResult();
        }

        /// <inheritdoc/>
        public IEnumerable<ValidationError> Validate(ValidationParameters validationParameters) => [];
    }
}