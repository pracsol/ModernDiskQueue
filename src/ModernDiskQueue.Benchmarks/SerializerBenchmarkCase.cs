// -----------------------------------------------------------------------
// <copyright file="SerializerCase.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Benchmarks
{
    using ModernDiskQueue.Benchmarks.SampleData;

    /// <summary>
    /// Record to hold the serialization strategy and queue path for each test case. This is used as a ParamSource for the benchmark.
    /// </summary>
    /// <param name="Strategy"><see cref="ISerializationStrategy{T}"/> to use for the benchmark.</param>
    /// <param name="QueuePath">The folder to use for the strategy, separate so we avoid collision.</param>
    public record SerializerBenchmarkCase(
        string Name,
        ISerializationStrategy<SampleDataObject> Strategy,
        string QueuePath);
}