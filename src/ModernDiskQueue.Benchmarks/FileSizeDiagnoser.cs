// -----------------------------------------------------------------------
// <copyright file="FileSizeDiagnoser.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Benchmarks
{
    using System.Collections.Concurrent;
    using System.Reflection;
    using BenchmarkDotNet.Analysers;
    using BenchmarkDotNet.Columns;
    using BenchmarkDotNet.Diagnosers;
    using BenchmarkDotNet.Engines;
    using BenchmarkDotNet.Exporters;
    using BenchmarkDotNet.Loggers;
    using BenchmarkDotNet.Reports;
    using BenchmarkDotNet.Running;
    using BenchmarkDotNet.Validators;
    using ModernDiskQueue.Benchmarks.Helpers;

    public class FileSizeDiagnoser : IDiagnoser
    {
        private List<(string StrategyName, int IterationCount, long FileSize)> fileSizeData;

        public IEnumerable<string> Ids => new[] { nameof(FileSizeDiagnoser) };

        public RunMode GetRunMode(BenchmarkCase benchmarkCase) => RunMode.NoOverhead;

        public bool RequiresBlockingAcknowledgment => false;

        public void DisplayResults(ILogger logger)
        {
            logger.WriteLine();
            logger.WriteLine("=== File Size Statistics ===");

            var statsByStrategy = fileSizeData
                .GroupBy(t => t.StrategyName)
                .Select(g => new
                {
                    StrategyName = g.Key,
                    Average = g.Average(x => x.FileSize),
                    Min = g.Min(x => x.FileSize),
                    Max = g.Max(x => x.FileSize),
                })
                .ToList();

            logger.WriteLine("Strategy Name                  | Average File Size | Min File Size | Max File Size");

            if (statsByStrategy.Count == 0)
            {
                logger.WriteLine("No data available.");
                return;
            }
            else
            {
                foreach (var stat in statsByStrategy)
                {
                    logger.WriteLine($"{stat.StrategyName} | {stat.Average} | {stat.Min} | {stat.Max}");
                }
            }

            logger.WriteLine($"Data file path was at: {Helpers.CsvFileHelper.GetFilePath()}");
            logger.WriteLine();

        }

        public void Handle(HostSignal signal, DiagnoserActionParameters parameters)
        {
            if (signal.Equals(HostSignal.AfterActualRun))
            {
                // Collect file size data after the actual run
                AfterActualRun(parameters);
            }

            // This is a fallback implementation that ensures we collect data
            // even if the direct method (AfterActualRun) isn't called
            // We can leave it empty since AfterActualRun is handling our logic
        }

        public IEnumerable<Metric> ProcessResults(DiagnoserResults results)
        {
            var statsByStrategy = fileSizeData
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
                yield return new Metric(new FileSizeMinMetricDescriptor(stat.StrategyName), stat.Min);
                yield return new Metric(new FileSizeMaxMetricDescriptor(stat.StrategyName), stat.Max);
            }
        }

        // Required interface implementation methods
        public void BeforeAnythingElse(DiagnoserActionParameters parameters) { }

        public void BeforeActualRun(DiagnoserActionParameters parameters) { }

        public void AfterActualRun(DiagnoserActionParameters parameters)
        {
            Console.WriteLine($"Collecting data from file {CsvFileHelper.GetFilePath()}...");
            try
            {
                fileSizeData = CsvFileHelper.ReadCsvToTupleList();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error reading file size data: {ex.Message}");
                fileSizeData = new List<(string, int, long)>();
            }

            Console.WriteLine($"File size data collected: {fileSizeData.Count} entries.");
        }

        public void BeforeGlobalSetup(DiagnoserActionParameters parameters) { }

        public void AfterGlobalSetup(DiagnoserActionParameters parameters) { }

        public void BeforeGlobalCleanup(DiagnoserActionParameters parameters) { }

        public void AfterGlobalCleanup(DiagnoserActionParameters parameters) { }

        public void BeforeMainRun(DiagnoserActionParameters parameters) { }

        public void AfterMainRun(DiagnoserActionParameters parameters) { }

        public void BeforeCleanup(DiagnoserActionParameters parameters) { }

        public void AfterCleanup(DiagnoserActionParameters parameters) { }

        public void BeforeSetup(DiagnoserActionParameters parameters) { }

        public void AfterSetup(DiagnoserActionParameters parameters) { }

        public IEnumerable<ValidationError> Validate(ValidationParameters validationParameters) => Array.Empty<ValidationError>();

        public IEnumerable<IExporter> Exporters => Array.Empty<IExporter>();

        public IEnumerable<IAnalyser> Analysers => Array.Empty<IAnalyser>();

        private string GetBenchmarkKey(BenchmarkCase benchmarkCase)
        {
            var methodName = benchmarkCase.Descriptor.WorkloadMethod.Name;

            // Include parameter values in the key
            var paramValues = benchmarkCase.Parameters.Items
            .Select(p => $"{p.Name}={p.Value}")
            .ToList();

            var paramInfo = paramValues.Any() ? $"[{string.Join(",", paramValues)}]" : string.Empty;

            return $"{methodName}{paramInfo}";
        }
    }

    public class FileSizeAvgMetricDescriptor : IMetricDescriptor
    {
        private readonly string _strategy;

        public FileSizeAvgMetricDescriptor(string strategy) => _strategy = strategy;

        public string Id => $"FileSizeAvg[{_strategy}]";

        public string DisplayName => $"Avg File Size ({_strategy})";

        public string Legend => $"Average file size for {_strategy}";

        public bool TheGreaterTheBetter => false;

        public string NumberFormat => "N0";

        public UnitType UnitType => UnitType.Size;

        public string Unit => "B";

        public int PriorityInCategory => 0;

        public bool GetIsAvailable(Metric metric) => true;
    }

    public class FileSizeMinMetricDescriptor : IMetricDescriptor
    {
        private readonly string _strategy;

        public FileSizeMinMetricDescriptor(string strategy) => _strategy = strategy;

        public string Id => $"FileSizeMin[{_strategy}]";

        public string DisplayName => $"Min File Size ({_strategy})";

        public string Legend => $"Minimum file size for {_strategy}";

        public bool TheGreaterTheBetter => false;

        public string NumberFormat => "N0";

        public UnitType UnitType => UnitType.Size;

        public string Unit => "B";

        public int PriorityInCategory => 0;

        public bool GetIsAvailable(Metric metric) => true;
    }

    public class FileSizeMaxMetricDescriptor : IMetricDescriptor
    {
        private readonly string _strategy;

        public FileSizeMaxMetricDescriptor(string strategy) => _strategy = strategy;

        public string Id => $"FileSizeMax[{_strategy}]";

        public string DisplayName => $"Max File Size ({_strategy})";

        public string Legend => $"Maximum file size for {_strategy}";

        public bool TheGreaterTheBetter => false;

        public string NumberFormat => "N0";

        public UnitType UnitType => UnitType.Size;

        public string Unit => "B";

        public int PriorityInCategory => 0;

        public bool GetIsAvailable(Metric metric) => true;
    }
}