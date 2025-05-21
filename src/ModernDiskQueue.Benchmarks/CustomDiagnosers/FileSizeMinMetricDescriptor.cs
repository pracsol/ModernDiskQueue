// -----------------------------------------------------------------------
// <copyright file="FileSizeMinMetricDescriptor.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Benchmarks.CustomDiagnosers
{
    using System.Text;
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

    public class FileSizeMinMetricDescriptor : IMetricDescriptor
    {
        private readonly string _strategy;

        public FileSizeMinMetricDescriptor(string strategy) => _strategy = strategy;

        public string Id => $"FileSizeMin[{_strategy}]";

        public string DisplayName => $"Min File Size ({_strategy})";

        public string Legend => $"Minimum file size for {_strategy}";

        public bool TheGreaterTheBetter => false;

        public string NumberFormat => "N2";

        public UnitType UnitType => UnitType.Size;

        public string Unit => "b";

        public int PriorityInCategory => 0;

        public bool GetIsAvailable(Metric metric) => true;
    }
}