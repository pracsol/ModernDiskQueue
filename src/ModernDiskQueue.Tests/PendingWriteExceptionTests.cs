// <copyright file="PendingWriteExceptionTests.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>

namespace ModernDiskQueue.Tests
{
    using System;
    using System.Linq;
    using ModernDiskQueue.Implementation;
    using NUnit.Framework;

    [TestFixture]
    public class PendingWriteExceptionTests
    {
        [Test]
        public void Can_get_all_information_from_to_string()
        {
            try
            {
                throw new ArgumentException("foo");
            }
            catch (Exception e)
            {
                var s = new PendingWriteException([e]).ToString();
                Assert.That(
                    s.Contains(e.ToString()), Is.True);
            }
        }

        [Test]
        public void Can_get_exception_detail_information_from_pending_write_exception()
        {
            try
            {
                throw new ArgumentException("foo");
            }
            catch (Exception e)
            {
                var s = new PendingWriteException([e]);
                Assert.That(
                    s.PendingWritesExceptions.Contains(e),
                    Is.True);
            }
        }
    }
}