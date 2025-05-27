// -----------------------------------------------------------------------
// <copyright file="TestClassSlim.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Tests.Models
{
    using System;
    using ModernDiskQueue;

    /// <summary>
    /// Test class for tests on <see cref="PersistentQueue{T}"/>.
    /// </summary>
    public class TestClassSlim : IEquatable<TestClassSlim>
    {
        public TestClassSlim(int integerValue)
        {
            IntegerValue = integerValue;
        }

        private int _integerField = 0;

        //public object ArbitraryObjectType { get; set; } = new object();

        //public dynamic ArbitraryDynamicType { get; set; } = string.Empty;

        public int IntegerValue { get; set; }

        public string StringValue { get; set; } = string.Empty;

        public System.Net.IPAddress IPAddress { get; set; } = System.Net.IPAddress.Loopback;

        public int? NullableIntegerValue { get; } = null;

        public DateTimeOffset TimeOffset { get; set; } = DateTimeOffset.Now;

        public DateTime Time { get; set; } = DateTime.Now;

        public static bool operator ==(TestClassSlim left, TestClassSlim right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(TestClassSlim left, TestClassSlim right)
        {
            return !Equals(left, right);
        }

        public bool Equals(TestClassSlim? other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            return IntegerValue == other.IntegerValue
                && StringValue == other.StringValue
                && NullableIntegerValue == other.NullableIntegerValue
                && (TimeOffset - other.TimeOffset).Duration() < TimeSpan.FromSeconds(1)
                && (Time - other.Time).Duration() < TimeSpan.FromSeconds(1);
        }

        public override bool Equals(object? obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj.GetType() != GetType())
            {
                return false;
            }

            return Equals((TestClassSlim)obj);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(IntegerValue, StringValue, NullableIntegerValue);
        }

        /// <summary>
        /// Override ToString so that the contents can easily be inspected in the debugger.
        /// </summary>
        public override string ToString()
        {
            return $"{IntegerValue}|{StringValue}|{NullableIntegerValue}";
        }

        public int GetInternalField()
        {
            return _integerField;
        }

        public void SetInternalField(int value)
        {
            _integerField = value;
        }
    }
}
