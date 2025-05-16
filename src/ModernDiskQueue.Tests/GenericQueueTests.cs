// <copyright file="GenericQueueTests.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>

// ReSharper disable AssignNullToNotNullAttribute
namespace ModernDiskQueue.Tests
{
    using System;
    using NUnit.Framework;

    [TestFixture]
    [SingleThreaded]
    public class GenericQueueTests
    {
        // Note: having the queue files shared between the tests checks that we
        // are correctly closing down the queue (i.e. the `Dispose()` call works)
        // If not, one of the files will fail complaining that the lock is still held.
        private const string QueueName = "./GenericQueueTest";

        [Test]
        public void Round_trip_value_type()
        {
            using var queue = new PersistentQueue<int>(QueueName + "int");
            using var session = queue.OpenSession();

            session.Enqueue(7);
            session.Flush();
            var testNumber = session.Dequeue();
            session.Flush();
            Assert.That(7, Is.EqualTo(testNumber));
        }

        [TestCase("Test")]
        [TestCase("")]
        [TestCase(" Leading Spaces")]
        [TestCase("Trailing Spaces   ")]
        [TestCase("A string longer than the others but still quite short")]
        [TestCase("other \r\n\t\b characters")]
        public void Round_trip_string_type(string valueToTest)
        {
            // Use different queue for each test case so that we don't get errors when running tests concurrently.
            var hash = valueToTest.GetHashCode().ToString("X8");
            using var queue = new PersistentQueue<string>($"./GenericQueueTests3{hash}");
            using var session = queue.OpenSession();

            while (session.Dequeue() != null)
            {
                Console.WriteLine("Removing old data");
            }

            session.Flush();

            session.Enqueue(valueToTest);
            session.Flush();
            var returnValue = session.Dequeue();
            session.Flush();
            Assert.That(valueToTest, Is.EqualTo(returnValue));
        }

        [Test]
        public void Round_trip_complex_type()
        {
            using var queue = new PersistentQueue<TestClass>(QueueName + "TC");
            using var session = queue.OpenSession();

            var testObject = new TestClass(7, "TestString", null);
            session.Enqueue(testObject);
            session.Flush();
            var testObject2 = session.Dequeue();
            session.Flush();
            Assert.That(testObject2, Is.Not.Null);
            Assert.That(testObject, Is.EqualTo(testObject2));

            testObject = new TestClass(7, "TestString", -5);
            session.Enqueue(testObject);
            session.Flush();
            testObject2 = session.Dequeue();
            session.Flush();
            Assert.That(testObject2, Is.Not.Null);
            Assert.That(testObject, Is.EqualTo(testObject2));
        }

        [Test]
        public void Round_trip_DateTimeOffset()
        {
            using var queue = new PersistentQueue<DateTimeOffset>(QueueName + "TC2");
            using var session = queue.OpenSession();

            var testObject = DateTimeOffset.Now;
            session.Enqueue(testObject);
            session.Flush();
            var testObject2 = session.Dequeue();
            session.Flush();
            Assert.That(testObject2, Is.Not.Null);
            Assert.That(testObject, Is.EqualTo(testObject2));
        }
    }
}