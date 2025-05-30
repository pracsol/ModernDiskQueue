﻿// -----------------------------------------------------------------------
// <copyright file="FileSizeMaxMetricDescriptor.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Benchmarks.CustomDiagnosers
{
    using BenchmarkDotNet.Columns;
    using BenchmarkDotNet.Reports;

    public class FileSizeMaxMetricDescriptor : IMetricDescriptor
    {
        private readonly string _strategy;

        public FileSizeMaxMetricDescriptor(string strategy) => _strategy = strategy;

        public string Id => $"FileSizeMax[{_strategy}]";

        public string DisplayName => $"Max File Size ({_strategy})";

        public string Legend => $"Maximum file size for {_strategy}";

        public bool TheGreaterTheBetter => false;

        public string NumberFormat => "N2";

        public UnitType UnitType => UnitType.Size;

        public string Unit => "b";

        public int PriorityInCategory => 0;

        public bool GetIsAvailable(Metric metric) => true;
    }
}