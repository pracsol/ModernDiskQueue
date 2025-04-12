using System;
// ReSharper disable AssignNullToNotNullAttribute

namespace ModernDiskQueue.Tests
{
    /// <summary>
    /// Test class for tests on <see cref="PersistentQueue{T}"/>
    /// </summary>
    [Serializable]
    public class TestClass : IEquatable<TestClass>
    {
        public TestClass(int integerValue, string stringValue, int? nullableIntegerValue)
        {
            IntegerValue = integerValue;
            StringValue = stringValue;
            NullableIntegerValue = nullableIntegerValue;
            TimeOffset = DateTimeOffset.Now;
            Time = DateTime.Now;
        }

        public int IntegerValue { get; }
        public string StringValue { get; }
        public int? NullableIntegerValue { get; }
        public DateTimeOffset TimeOffset { get; }
        public DateTime Time { get; set; }

        public bool Equals(TestClass? other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;

            return IntegerValue == other.IntegerValue
                && StringValue == other.StringValue
                && NullableIntegerValue == other.NullableIntegerValue
                && (TimeOffset - other.TimeOffset).Duration() < TimeSpan.FromSeconds(1)
                && (Time - other.Time).Duration() < TimeSpan.FromSeconds(1);
        }

        public override bool Equals(object? obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((TestClass)obj);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(IntegerValue, StringValue, NullableIntegerValue);
        }

        public static bool operator ==(TestClass left, TestClass right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(TestClass left, TestClass right)
        {
            return !Equals(left, right);
        }

        /// <summary>
        /// Override ToString so that the contents can easily be inspected in the debugger.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return $"{IntegerValue}|{StringValue}|{NullableIntegerValue}";
        }
    }
}