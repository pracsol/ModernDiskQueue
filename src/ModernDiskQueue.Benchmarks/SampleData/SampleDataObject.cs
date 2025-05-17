// -----------------------------------------------------------------------
// <copyright file="SampleDataObject.cs" company="ModernDiskQueue Contributors">
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

    public class SampleDataObject
    {
        // Primitive Types
        public int IntegerValue { get; set; }

        public long LongValue { get; set; }

        public float FloatValue { get; set; }

        public double DoubleValue { get; set; }

        public decimal DecimalValue { get; set; }

        public bool BooleanValue { get; set; }

        public char CharValue { get; set; }

        public string StringValue { get; set; }

        public byte ByteValue { get; set; }

        // Nullable Types
        public int? NullableInt { get; set; }

        public bool? NullableBool { get; set; }

        public DateTime? NullableDateTime { get; set; }

        // Date and Time
        public DateTime DateTimeValue { get; set; }

        public TimeSpan TimeSpanValue { get; set; }

        public DateOnly DateOnlyValue { get; set; }

        public TimeOnly TimeOnlyValue { get; set; }

        // Collections
        public int[] IntArray { get; set; }

        public List<string> StringList { get; set; }

        public Dictionary<string, double> StringDoubleDictionary { get; set; }

        // Nested Objects
        public NestedData NestedObject { get; set; }

        // Enum
        public SampleEnum EnumValue { get; set; }

        // Object and Dynamic
        public object ArbitraryObject { get; set; }

        public dynamic DynamicValue { get; set; }

        // Complex collection
        public List<NestedData> NestedList { get; set; }
    }
}