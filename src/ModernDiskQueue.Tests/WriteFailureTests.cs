// -----------------------------------------------------------------------
// <copyright file="WriteFailureTests.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Tests
{
    using System.IO;
    using ModernDiskQueue.Implementation;
    using NUnit.Framework;

    [TestFixture]
    public class WriteFailureTests : PersistentQueueTestsBase
    {
        protected override string QueuePath => "./WriteFailureTests";

        [Test]
        public void EnqueueFailsIfDiskIsFullButDequeueStillWorks()
        {
            using var subject = new PersistentQueue(QueuePath);
            subject.HardDelete(true);

            using (var session = subject.OpenSession())
            {
                for (int i = 0; i < 100; i++)
                {
                    session.Enqueue(new byte[] { 1, 2, 3, 4 });
                    session.Flush();
                }
            }

            using (var session = subject.OpenSession())
            {
                // Switch to a file system that fails on write.
                subject.Internals.SetFileDriver(new WriteFailureDriver());

                for (int i = 0; i < 3; i++)
                {
                    var result = session.Dequeue();
                    Assert.That(result, Is.Not.Null);

                    session.Enqueue(new byte[] { 1, 2, 3, 4 });
                    Assert.Throws<IOException>(() => { session.Flush(); }, "should have thrown an exception when trying to write");
                }
            }

            // Restore driver so we can dispose correctly.
            subject.Internals.SetFileDriver(new StandardFileDriver());
        }
    }
}