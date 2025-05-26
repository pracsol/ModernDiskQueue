// -----------------------------------------------------------------------
// <copyright file="Program.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Benchmarks
{
    using BenchmarkDotNet.Running;

    public static class Program
    {
        public static void Main(string[] args)
        {
            BenchmarkRunner.Run(new[]
            {
                // typeof(ContentiousEnqueues),
                // typeof(HighVolumeEnqueues),
                // typeof(ThreadsAndTasks),
                typeof(SerializerStrategies),
            });
        }
    }
}
