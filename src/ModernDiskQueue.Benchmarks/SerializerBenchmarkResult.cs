// -----------------------------------------------------------------------
// <copyright file="SerializerBenchmarkResult.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Benchmarks
{
    /// <summary>
    /// Record to hold the results of the serializer benchmark.
    /// </summary>
    public class SerializerBenchmarkResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SerializerBenchmarkResult"/> class.
        /// </summary>
        public SerializerBenchmarkResult() { }

        /// <summary>
        /// Gets or sets a friendly name of the strategy displayed in results.
        /// </summary>
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the name of the serialization strategy, e.g. nameof([SerializationStrategyClass]).
        /// </summary>
        public string TypeName { get; set; } = string.Empty;

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