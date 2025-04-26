using ModernDiskQueue.Implementation;
using ModernDiskQueue.PublicInterfaces;
using ModernDiskQueue.Tests.Helpers;
using NSubstitute;
using NSubstitute.Core;
using NUnit.Framework;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

// ReSharper disable PossibleNullReferenceException

namespace ModernDiskQueue.Tests
{
    [TestFixture, SingleThreaded]
    public class PersistentQueueSessionTests : PersistentQueueTestsBase
    {
        protected override string QueuePath => "./PersistentQueueSessionTest";

        [Test]
        public void Errors_raised_during_pending_write_will_be_thrown_on_flush()
        {
            var loggerFactory = Substitute.For<ILoggerFactory>();
            var limitedSizeStream = new MemoryStream(new byte[4]);
            var fileStream = new FileStreamWrapper(limitedSizeStream);
            var queueStub = PersistentQueueWithMemoryStream(fileStream);

            var pendingWriteException = Assert.Throws<AggregateException>(() =>
            {
                // Create a session with a write buffer size of 1,048,576
                using (var session = new PersistentQueueSession(loggerFactory, queueStub, fileStream, 1024 * 1024, 1000))
                {
                    // Send in an excessively large amount of data to write, 67,000,000+.
                    // This will exceed the write buffer and the size of the stream.
                    // An exception will be thrown during the enqueue operation because
                    // the data exceeds the write buffer size. However, the exception will 
                    // be stored in a collection of pending write failures, and returned as
                    // an aggregate exception during the Flush operation.
                    session.Enqueue(new byte[64 * 1024 * 1024 + 1]);
                    session.Flush();
                }
            });

            Assert.That(pendingWriteException.Message, Is.EqualTo("One or more errors occurred. (Error during pending writes:" + Environment.NewLine + " - Memory stream is not expandable.)"));
        }

        [Test]
        public void Errors_raised_during_flush_write_will_be_thrown_as_is()
        {
            var loggerFactory = Substitute.For<ILoggerFactory>();
            var limitedSizeStream = new MemoryStream(new byte[4]);
            var fileStream = new FileStreamWrapper(limitedSizeStream);
            var queueStub = PersistentQueueWithMemoryStream(fileStream);

            var notSupportedException = Assert.Throws<NotSupportedException>(() =>
            {
                // Create a session with a write buffer size of 1,048,576
                using (var session = new PersistentQueueSession(loggerFactory, queueStub, fileStream, 1024 * 1024, 1000))
                {
                    // Send in a small amount of data to write, which is less than
                    // the write buffer size, but greater than the stream size.
                    // The enqueue operation will succeed, but an exception will be thrown
                    // when the flush operation happens because the data is to large for the stream size.
                    session.Enqueue(new byte[64]);
                    session.Flush();
                }
            });

            Assert.That(notSupportedException.Message, Is.EqualTo(@"Memory stream is not expandable."));
        }

        [Test]
        public void If_data_stream_is_truncated_will_raise_error()
        {
            using (var queue = new PersistentQueue(QueuePath))
            using (var session = queue.OpenSession())
            {
                session.Enqueue(new byte[] { 1, 2, 3, 4 });
                session.Flush();
            }
            using (var fs = new FileStream(System.IO.Path.Combine(QueuePath, "data.0"), FileMode.Open))
            {
                fs.SetLength(2);//corrupt the file
            }

            Assert.Throws<InvalidOperationException>(() =>
            {
                using (var queue = new PersistentQueue(QueuePath))
                {
                    using (var session = queue.OpenSession())
                    {
                        session.Dequeue();
                    }
                }
            });
        }

        [Test]
        public void If_data_stream_is_truncated_will_NOT_raise_error_if_truncated_entries_are_allowed_in_settings()
        {
            PersistentQueue.DefaultSettings.AllowTruncatedEntries = true;
            PersistentQueue.DefaultSettings.ParanoidFlushing = true;

            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    session.Enqueue(new byte[] { 1, 2, 3, 4 });
                    session.Flush();
                }
            }
            using (var fs = new FileStream(System.IO.Path.Combine(QueuePath, "data.0"), FileMode.Open))
            {
                fs.SetLength(2);//corrupt the file
            }

            byte[]? bytes;
            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    bytes = session.Dequeue();
                }
            }

            PersistentQueue.DefaultSettings.AllowTruncatedEntries = false; // reset to default
            Assert.That(bytes, Is.Null);
        }

        [Test]
        public void If_data_stream_is_truncated_the_queue_can_still_be_used()
        {
            PersistentQueue.DefaultSettings.AllowTruncatedEntries = true;

            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    session.Enqueue(new byte[] { 1, 2, 3, 4 });
                    session.Flush();
                }
            }
            using (var fs = new FileStream(System.IO.Path.Combine(QueuePath, "data.0"), FileMode.Open))
            {
                fs.SetLength(2);//corrupt the file
            }

            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    session.Enqueue(new byte[] { 5, 6, 7, 8 });
                    session.Flush();
                }
            }

            byte[]? bytes, corruptBytes;
            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    corruptBytes = session.Dequeue();
                    bytes = session.Dequeue();
                }
            }

            Console.WriteLine(string.Join(", ", corruptBytes.OrEmpty().Select(b => b.ToString())));
            Console.WriteLine(string.Join(", ", bytes.OrEmpty().Select(b => b.ToString())));
            Assert.That(bytes!, Is.EqualTo(new byte[] { 5, 6, 7, 8 }));
        }

        private static IPersistentQueueImpl PersistentQueueWithMemoryStream(IFileStream limitedSizeStream)
        {
            var queueStub = Substitute.For<IPersistentQueueImpl>();

            queueStub.WhenForAnyArgs(x => x.AcquireWriter(default!, default!, default!))
                .Do(c => CallActionArgument(c, limitedSizeStream));
            return queueStub!;
        }

        private static void CallActionArgument(CallInfo c, IFileStream ms)
        {
            ((Func<IFileStream, Task<long>>)c.Args()[1]!)(ms);
        }
    }
}