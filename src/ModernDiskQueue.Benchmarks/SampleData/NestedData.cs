// -----------------------------------------------------------------------
// <copyright file="SampleDataFactory.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Benchmarks.SampleData
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using static ModernDiskQueue.Benchmarks.SerializerStrategies;

    public class NestedData
    {
        public Guid Id { get; set; }

        public string Name { get; set; }

        public Uri Website { get; set; }
    }
}