// <copyright file="TestClass.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>

// ReSharper disable AssignNullToNotNullAttribute
namespace ModernDiskQueue.Tests.Models
{
    using System;
    using ModernDiskQueue;

    /// <summary>
    /// Test class for tests on <see cref="PersistentQueue{T}"/>.
    /// </summary>
    [Serializable]
    public class TestClass : IEquatable<TestClass>
    {
        private int _integerField = 0;

        public TestClass(int integerValue, string stringValue, int? nullableIntegerValue)
        {
            IntegerValue = integerValue;
            StringValue = stringValue;
            NullableIntegerValue = nullableIntegerValue;
            TimeOffset = DateTimeOffset.Now;
            Time = DateTime.Now;
            ArbitraryObjectType = new object();
            ArbitraryDynamicType = stringValue;
        }

        public object ArbitraryObjectType { get; set; }

        public dynamic ArbitraryDynamicType { get; set; }

        public int IntegerValue { get; }

        public string StringValue { get; }

        public int? NullableIntegerValue { get; }

        public DateTimeOffset TimeOffset { get; set; }

        public DateTime Time { get; set; }

        public static bool operator ==(TestClass left, TestClass right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(TestClass left, TestClass right)
        {
            return !Equals(left, right);
        }

        public bool Equals(TestClass? other)
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

            return Equals((TestClass)obj);
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