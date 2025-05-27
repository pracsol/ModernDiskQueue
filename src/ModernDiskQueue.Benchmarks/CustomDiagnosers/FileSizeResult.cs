// -----------------------------------------------------------------------
// <copyright file="FileSizeResult.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Benchmarks.CustomDiagnosers
{
    /// <summary>
    /// Record to hold the results of the serializer benchmark.
    /// </summary>
    public class FileSizeResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="FileSizeResult"/> class.
        /// </summary>
        public FileSizeResult() { }

        /// <summary>
        /// Gets or sets a name for the results to be grouped under.
        /// </summary>
        public string GroupName { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the iteration number that generated this result.
        /// </summary>
        public int IterationCount { get; set; }

        /// <summary>
        /// Gets or sets the size of the data file created by the iteration.
        /// </summary>
        public long FileSize { get; set; }
    }
}