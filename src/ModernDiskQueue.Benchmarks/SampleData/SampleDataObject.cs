// -----------------------------------------------------------------------
// <copyright file="SampleDataObject.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Benchmarks.SampleData
{
    using System;
    using System.Collections.Generic;
    using MessagePack;

    [MessagePackObject]
    public class SampleDataObject
    {
        public SampleDataObject()
        {
            StringList = [];
            StringDoubleDictionary = [];
            NestedList = [];
            NestedObject = new NestedData();

            IntegerValue = 0;
            LongValue = default;
            FloatValue = default;
            DoubleValue = default;
            DecimalValue = default;
            BooleanValue = default;
            CharValue = default;
            StringValue = string.Empty;
            ByteValue = default;

            NullableInt = default;
            NullableBool = default;
            NullableDateTime = default;

            DateTimeValue = default;
            TimeSpanValue = default;
            DateOnlyValue = default;
            TimeOnlyValue = default;

            IntArray = [];

            EnumValue = default;

            // ArbitraryObject = new object();
            // DynamicValue = 0;
        }

        // Primitive Types
        [Key(0)]
        public int IntegerValue { get; set; }

        [Key(1)]
        public long LongValue { get; set; }

        [Key(2)]
        public float FloatValue { get; set; }

        [Key(3)]
        public double DoubleValue { get; set; }

        [Key(4)]
        public decimal DecimalValue { get; set; }

        [Key(5)]
        public bool BooleanValue { get; set; }

        [Key(6)]
        public char CharValue { get; set; }

        [Key(7)]
        public string StringValue { get; set; } = string.Empty;

        [Key(8)]
        public byte ByteValue { get; set; }

        // Nullable Types
        [Key(9)]
        public int? NullableInt { get; set; }

        [Key(10)]
        public bool? NullableBool { get; set; }

        [Key(11)]
        public DateTime? NullableDateTime { get; set; }

        // Date and Time
        [Key(12)]
        public DateTime DateTimeValue { get; set; }

        [Key(13)]
        public TimeSpan TimeSpanValue { get; set; }

        [Key(14)]
        public DateOnly DateOnlyValue { get; set; }

        [Key(15)]
        public TimeOnly TimeOnlyValue { get; set; }

        // Collections
        [Key(16)]
        public int[] IntArray { get; set; }

        [Key(17)]
        public List<string> StringList { get; set; }

        [Key(18)]
        public Dictionary<string, double> StringDoubleDictionary { get; set; }

        // Nested Objects
        [Key(19)]
        public NestedData NestedObject { get; set; }

        // Enum
        [Key(20)]
        public SampleEnum EnumValue { get; set; }

        // Object and Dynamic
        // public object ArbitraryObject { get; set; }

        // public dynamic DynamicValue { get; set; }

        // Complex collection
        [Key(21)]
        public List<NestedData> NestedList { get; set; }
    }
}