// -----------------------------------------------------------------------
// <copyright file="FileSizeAvgMetricDescriptor.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Benchmarks.CustomDiagnosers
{
    using BenchmarkDotNet.Columns;
    using BenchmarkDotNet.Reports;

    public class FileSizeAvgMetricDescriptor : IMetricDescriptor
    {
        private readonly string _strategy;

        public FileSizeAvgMetricDescriptor(string strategy) => _strategy = strategy;

        public string Id => $"FileSizeAvg[{_strategy}]";

        public string DisplayName => $"Avg File Size ({_strategy})";

        public string Legend => $"Average file size for {_strategy}";

        public bool TheGreaterTheBetter => false;

        public string NumberFormat => "N3";

        public UnitType UnitType => UnitType.Size;

        public string Unit => "B";

        public int PriorityInCategory => 0;

        public bool GetIsAvailable(Metric metric) => true;
    }

    public class FileSizeAvgMetricDescriptor2 : IMetricDescriptor
    {
        public string Id => $"FileSizeAvg";

        public string DisplayName => $"Avg File Size (KB)";

        public string Legend => $"Average file size of the Data.0 file";

        public bool TheGreaterTheBetter => false;

        public string NumberFormat => "N3";

        public UnitType UnitType => UnitType.Size;

        public string Unit => "KB";

        public int PriorityInCategory => 0;

        public bool GetIsAvailable(Metric metric) => true;
    }
}