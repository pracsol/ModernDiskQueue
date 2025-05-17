// -----------------------------------------------------------------------
// <copyright file="SampleDataFactory.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Benchmarks.SampleData
{
    using System;
    using System.Collections.Generic;

    public static class SampleDataFactory
    {
        private static readonly Random _rand = new Random();

        public static SampleDataObject CreateRandomSampleData()
        {
            return new SampleDataObject
            {
                // Primitive types
                IntegerValue = _rand.Next(),
                LongValue = (long)_rand.Next() << 32 | (uint)_rand.Next(),
                FloatValue = (float)_rand.NextDouble() * 100,
                DoubleValue = _rand.NextDouble() * 1000,
                DecimalValue = (decimal)_rand.NextDouble() * 10000,
                BooleanValue = _rand.Next(0, 2) == 0,
                CharValue = (char)_rand.Next('A', 'Z' + 1),
                StringValue = $"String_{Guid.NewGuid()}",
                ByteValue = (byte)_rand.Next(0, 256),

                // Nullable types
                NullableInt = _rand.Next(0, 2) == 0 ? null : _rand.Next(0, 100),
                NullableBool = _rand.Next(0, 2) == 0 ? null : _rand.Next(0, 2) == 0,
                NullableDateTime = _rand.Next(0, 2) == 0 ? null : DateTime.Now.AddDays(_rand.Next(-1000, 1000)),

                // Date and Time
                DateTimeValue = DateTime.UtcNow.AddMinutes(_rand.Next(-10000, 10000)),
                TimeSpanValue = TimeSpan.FromMinutes(_rand.Next(0, 1440)),
                DateOnlyValue = DateOnly.FromDateTime(DateTime.Today.AddDays(_rand.Next(-365, 365))),
                TimeOnlyValue = TimeOnly.FromDateTime(DateTime.Now.AddMinutes(_rand.Next(-720, 720))),

                // Collections
                IntArray = new[] { _rand.Next(0, 100), _rand.Next(100, 200), _rand.Next(200, 300) },
                StringList = new List<string> { $"Item_{_rand.Next(100)}", $"Item_{_rand.Next(100)}" },
                StringDoubleDictionary = new Dictionary<string, double>
                {
                    { "A", _rand.NextDouble() },
                    { "B", _rand.NextDouble() },
                },

                // Nested object
                NestedObject = CreateRandomNestedData(),

                // Enum
                EnumValue = (SampleEnum)_rand.Next(0, Enum.GetValues(typeof(SampleEnum)).Length),

                // Object and Dynamic
                ArbitraryObject = new { Key = "Random", Value = _rand.Next(1000) },
                DynamicValue = new { Foo = "Bar", Baz = _rand.Next(999) },

                // Complex Collection
                NestedList = new List<NestedData>
                {
                    CreateRandomNestedData(),
                    CreateRandomNestedData()
                },
            };
        }

        private static NestedData CreateRandomNestedData()
        {
            return new NestedData
            {
                Id = Guid.NewGuid(),
                Name = $"Name_{_rand.Next(1000)}",
                Website = new Uri($"https://example{_rand.Next(100)}.com"),
            };
        }
    }
}