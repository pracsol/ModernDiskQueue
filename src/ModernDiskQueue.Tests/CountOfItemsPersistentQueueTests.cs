// -----------------------------------------------------------------------
// <copyright file="CountOfItemsPersistentQueueTests.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Tests
{
    using NUnit.Framework;

    [TestFixture]
    [SingleThreaded]
    public class CountOfItemsPersistentQueueTests : PersistentQueueTestsBase
    {
        protected override string QueuePath => "./CountOfItemsTests";

        [Test]
        public void Can_get_count_from_queue()
        {
            using (var queue = new PersistentQueue(QueuePath))
            {
                Assert.That(0, Is.EqualTo(queue.EstimatedCountOfItemsInQueue));
            }
        }

        [Test]
        public void Can_enter_items_and_get_count_of_items()
        {
            using (var queue = new PersistentQueue(QueuePath))
            {
                for (byte i = 0; i < 5; i++)
                {
                    using (var session = queue.OpenSession())
                    {
                        session.Enqueue(new[] { i });
                        session.Flush();
                    }
                }

                Assert.That(5, Is.EqualTo(queue.EstimatedCountOfItemsInQueue));
            }
        }

        [Test]
        public void Can_get_count_of_items_after_queue_restart()
        {
            using (var queue = new PersistentQueue(QueuePath))
            {
                for (byte i = 0; i < 5; i++)
                {
                    using (var session = queue.OpenSession())
                    {
                        session.Enqueue(new[] { i });
                        session.Flush();
                    }
                }
            }

            using (var queue = new PersistentQueue(QueuePath))
            {
                Assert.That(5, Is.EqualTo(queue.EstimatedCountOfItemsInQueue));
            }
        }
    }
}