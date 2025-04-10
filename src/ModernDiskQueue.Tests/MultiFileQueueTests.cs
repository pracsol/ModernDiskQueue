using NUnit.Framework;
using System.IO;
// ReSharper disable AssignNullToNotNullAttribute

namespace ModernDiskQueue.Tests
{
    [TestFixture, SingleThreaded]
    public class MultiFileQueueTests : PersistentQueueTestsBase
    {
        protected override string Path => "./MultiFileQueue";

        [Test]
        public void Can_limit_amount_of_items_in_queue_file()
        {
            using (IPersistentQueue queue = new PersistentQueue(Path, 10))
            {
                Assert.That(10, Is.EqualTo(queue.MaxFileSize));
            }
        }

        [Test]
        public void Entering_more_than_count_of_items_will_work()
        {
            using (var queue = new PersistentQueue(Path, 10))
            {
                for (byte i = 0; i < 11; i++)
                {
                    using (var session = queue.OpenSession())
                    {
                        session.Enqueue(new[] { i });
                        session.Flush();
                    }
                }
                Assert.That(11, Is.EqualTo(queue.EstimatedCountOfItemsInQueue));
            }
        }

        [Test]
        public void When_creating_more_items_than_allowed_in_first_file_will_create_additional_file()
        {
            using (var queue = new PersistentQueue(Path, 10))
            {
                for (byte i = 0; i < 11; i++)
                {
                    using (var session = queue.OpenSession())
                    {
                        session.Enqueue(new[] { i });
                        session.Flush();
                    }
                }
                Assert.That(1, Is.EqualTo(queue.Internals.CurrentFileNumber));
            }
        }

        [Test]
        public void Can_resume_writing_to_second_file_when_restart_queue()
        {
            using (var queue = new PersistentQueue(Path, 10))
            {
                for (byte i = 0; i < 11; i++)
                {
                    using (var session = queue.OpenSession())
                    {
                        session.Enqueue(new[] { i });
                        session.Flush();
                    }
                }
                Assert.That(1, Is.EqualTo(queue.Internals.CurrentFileNumber));
            }
            using (var queue = new PersistentQueue(Path, 10))
            {
                for (byte i = 0; i < 2; i++)
                {
                    using (var session = queue.OpenSession())
                    {
                        session.Enqueue(new[] { i });
                        session.Flush();
                    }
                }
                Assert.That(1, Is.EqualTo(queue.Internals.CurrentFileNumber));
            }
        }

        [Test]
        public void Can_dequeue_from_all_files()
        {
            using (var queue = new PersistentQueue(Path, 10))
            {
                for (byte i = 0; i < 12; i++)
                {
                    using (var session = queue.OpenSession())
                    {
                        session.Enqueue(new[] { i });
                        session.Flush();
                    }
                }
                Assert.That(1, Is.EqualTo(queue.Internals.CurrentFileNumber));
            }

            using (var queue = new PersistentQueue(Path, 10))
            {
                for (byte i = 0; i < 12; i++)
                {
                    using (var session = queue.OpenSession())
                    {
                        Assert.That(i, Is.EqualTo(session.Dequeue()?[0]));
                        session.Flush();
                    }
                }
            }
        }

        [Test]
        public void Can_dequeue_from_all_files_after_restart()
        {
            using (var queue = new PersistentQueue(Path, 10))
            {
                for (byte i = 0; i < 12; i++)
                {
                    using (var session = queue.OpenSession())
                    {
                        session.Enqueue(new[] { i });
                        session.Flush();
                    }
                }
                Assert.That(1, Is.EqualTo(queue.Internals.CurrentFileNumber));
            }

            using (var queue = new PersistentQueue(Path, 10))
            {
                for (byte i = 0; i < 3; i++)
                {
                    using (var session = queue.OpenSession())
                    {
                        session.Enqueue(new[] { i });
                        session.Flush();
                    }
                }
                Assert.That(1, Is.EqualTo(queue.Internals.CurrentFileNumber));
            }


            using (var queue = new PersistentQueue(Path, 10))
            {
                using (var session = queue.OpenSession())
                {
                    for (byte i = 0; i < 12; i++)
                    {
                        Assert.That(i, Is.EqualTo(session.Dequeue()?[0]));
                        session.Flush();
                    }

                    for (byte i = 0; i < 3; i++)
                    {
                        Assert.That(i, Is.EqualTo(session.Dequeue()?[0]));
                        session.Flush();
                    }
                }
            }
        }

        [Test]
        public void After_reading_all_items_from_file_that_is_not_the_active_file_should_delete_file()
        {
            using (var queue = new PersistentQueue(Path, 10))
            {
                for (byte i = 0; i < 12; i++) // 12 individual bytes, and a 10 byte file limit
                {
                    using (var session = queue.OpenSession())
                    {
                        session.Enqueue(new[] { i });
                        session.Flush();
                    }
                }
                Assert.That(1, Is.EqualTo(queue.Internals.CurrentFileNumber));
            }

            using (var queue = new PersistentQueue(Path, 10))
            {
                for (byte i = 0; i < 12; i++)
                {
                    using (var session = queue.OpenSession())
                    {
                        Assert.That(i, Is.EqualTo(session.Dequeue()?[0]));
                        session.Flush();
                    }
                }
            }

            Assert.That(
                File.Exists(System.IO.Path.Combine(Path, "data.0")), Is.False
                );
        }
    }
}