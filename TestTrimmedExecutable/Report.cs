// -----------------------------------------------------------------------
// <copyright file="Report.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace TestTrimmedExecutable
{
    [Serializable]
    internal class Report
    {
        public Guid Id { get; set; } = Guid.Empty;

        public int ReportType { get; set; }

        public string DataField { get; set; } = string.Empty;

        public DateTimeOffset LocalTime { get; set; }
    }
}