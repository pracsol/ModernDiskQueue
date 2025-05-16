// <copyright file="ConstantChecks.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>

namespace ModernDiskQueue.Tests
{
    using ModernDiskQueue.Implementation;
    using NUnit.Framework;

    [TestFixture]
    public class ConstantChecks
    {
        [Test]
        public void StartTransactionSeparatorGuid_is_undamaged()
        {
            // This should never be changed! If this test fails existing queues will be unreadable.
            Assert.That("b75bfb12-93bb-42b6-acb1-a897239ea3a5", Is.EqualTo(Constants.StartTransactionSeparatorGuid.ToString()));
        }

        [Test]
        public void EndTransactionSeparatorGuid_is_undamaged()
        {
            // This should never be changed! If this test fails existing queues will be unreadable.
            Assert.That("866c9705-4456-4e9d-b452-3146b3bfa4ce", Is.EqualTo(Constants.EndTransactionSeparatorGuid.ToString()));
        }
    }
}