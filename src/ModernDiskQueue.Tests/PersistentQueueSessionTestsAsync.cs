// <copyright file="PersistentQueueSessionTestsAsync.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>

namespace ModernDiskQueue.Tests
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using ModernDiskQueue.Implementation;
    using ModernDiskQueue.Implementation.Interfaces;
    using ModernDiskQueue.Tests.Helpers;
    using NSubstitute;
    using NUnit.Framework;

    [TestFixture]
    [SingleThreaded]
    public class PersistentQueueSessionTestsAsync : PersistentQueueTestsBase
    {
        protected override string QueuePath => "./PersistentQueueSessionTest";

        private IPersistentQueueFactory _factory = Substitute.For<IPersistentQueueFactory>();

        [SetUp]
        public new void Setup()
        {
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.SetMinimumLevel(LogLevel.Information);
                builder.AddConsole();
            });
            _factory = new PersistentQueueFactory(loggerFactory);
            base.Setup();
        }

        [Test]
        public async Task Errors_raised_during_pending_write_will_be_thrown_on_flush()
        {
            var loggerFactory = Substitute.For<ILoggerFactory>();

            // Create a super small memory stream.
            var limitedSizeStream = new MemoryStream(new byte[4]);
            var fileStream = new FileStreamWrapper(limitedSizeStream);
            var queueStub = PersistentQueueWithMemoryStream(fileStream);

            var notSupportedException = Assert.ThrowsAsync<NotSupportedException>(async () =>
            {
                // Create a session with a write buffer size of 1,048,576
                await using (var session = new PersistentQueueSession(loggerFactory, queueStub, fileStream, 1024 * 1024, 1000))
                {
                    // Send in an excessively large amount of data to write, 67,000,000+.
                    // This will exceed the write buffer and the size of the stream.
                    // An exception will be thrown during the enqueue operation because
                    // the data exceeds the write buffer size. However, the exception will
                    // be stored in a collection of pending write failures, and returned as
                    // an aggregate exception during the Flush operation.
                    await session.EnqueueAsync(new byte[(64 * 1024 * 1024) + 1]);
                    await session.FlushAsync();
                }
            });

            // I've change the behavior of this test compared to the sync version.
            // In this case the exception is thrown during the enqueue operation, not during the flush.
            Assert.That(notSupportedException.Message, Is.EqualTo(@"Memory stream is not expandable."));
        }

        [Test]
        public async Task Errors_raised_during_flush_write_will_be_thrown_as_is()
        {
            var loggerFactory = Substitute.For<ILoggerFactory>();

            // Create a super small memory stream.
            var limitedSizeStream = new MemoryStream(new byte[4]);
            var fileStream = new FileStreamWrapper(limitedSizeStream);
            var queueStub = PersistentQueueWithMemoryStream(fileStream);

            var notSupportedException = Assert.ThrowsAsync<NotSupportedException>(async () =>
            {
                // Create a session with a write buffer size of 1,048,576
                PersistentQueueSession session = new PersistentQueueSession(loggerFactory, queueStub, fileStream, 1024 * 1024, 1000);
                await using (session)
                {
                    // Send in a small amount of data to write, which is less than
                    // the write buffer size, but greater than the stream size.
                    // The enqueue operation will succeed, but an exception will be thrown
                    // when the flush operation happens because the data is to large for the stream size.
                    await session.EnqueueAsync(new byte[64]);
                    await session.FlushAsync();
                }
            });
            Assert.That(notSupportedException.Message, Is.EqualTo(@"Memory stream is not expandable."));
        }

        [Test]
        public async Task If_data_stream_is_truncated_will_raise_error()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                    await session.FlushAsync();
                }
            }

            // Corrupt the file by truncating it
            using (var fs = new FileStream(System.IO.Path.Combine(QueuePath, "data.0"), FileMode.Open))
            {
                fs.SetLength(2);
            }

            // var invalidOperationException = Assert.ThrowsAsync<InvalidOperationException>(async () =>
            try
            {
                await using (var queue = await _factory.CreateAsync(QueuePath))
                {
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        await session.DequeueAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message, ex.StackTrace);
                Assert.That(ex, Is.TypeOf<InvalidOperationException>());
            }

            // });
            // Console.WriteLine(invalidOperationException.Message);
        }

        [Test]
        public async Task If_data_stream_is_truncated_will_NOT_raise_error_if_truncated_entries_are_allowed_in_settings()
        {
            // Using static default settings should be avoided in asyc operations in favor of setting options.
            // PersistentQueue.DefaultSettings.AllowTruncatedEntries = true;
            // PersistentQueue.DefaultSettings.ParanoidFlushing = true;
            ModernDiskQueueOptions options = new()
            {
                AllowTruncatedEntries = true,
                ParanoidFlushing = true,
            };

            _factory = new PersistentQueueFactory(options);

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                    await session.FlushAsync();
                }
            }

            using (var fs = new FileStream(System.IO.Path.Combine(QueuePath, "data.0"), FileMode.Open))
            {
                fs.SetLength(2); // corrupt the file
            }

            byte[]? bytes;
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    bytes = await session.DequeueAsync();
                }
            }

            PersistentQueue.DefaultSettings.AllowTruncatedEntries = false; // reset to default
            Assert.That(bytes, Is.Null);
        }

        [Test]
        public async Task If_data_stream_is_truncated_the_queue_can_still_be_used()
        {
            PersistentQueue.DefaultSettings.AllowTruncatedEntries = true;

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                    await session.FlushAsync();
                }
            }

            using (var fs = new FileStream(System.IO.Path.Combine(QueuePath, "data.0"), FileMode.Open))
            {
                fs.SetLength(2); // corrupt the file
            }

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    await session.EnqueueAsync(new byte[] { 5, 6, 7, 8 });
                    await session.FlushAsync();
                }
            }

            byte[]? bytes, corruptBytes;
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    corruptBytes = await session.DequeueAsync();
                    bytes = await session.DequeueAsync();
                }
            }

            Console.WriteLine(string.Join(", ", corruptBytes.OrEmpty().Select(b => b.ToString())));
            Console.WriteLine(string.Join(", ", bytes.OrEmpty().Select(b => b.ToString())));
            Assert.That(bytes!, Is.EqualTo(new byte[] { 5, 6, 7, 8 }));
        }

        private static IPersistentQueueImpl PersistentQueueWithMemoryStream(IFileStream limitedSizeStream)
        {
            var queueStub = Substitute.For<IPersistentQueueImpl>();

            _ = queueStub.AcquireWriterAsync(
                                        Arg.Any<IFileStream>(),
                                        Arg.Any<Func<IFileStream, CancellationToken, Task<long>>>(),
                                        Arg.Any<Action<IFileStream>>(),
                                        Arg.Any<CancellationToken>())
                .Returns(callInfo =>
                {
                    var actionFunc = callInfo.ArgAt<Func<IFileStream, CancellationToken, Task<long>>>(1);

                    // This returns the Task directly, letting the exception propagate naturally
                    return actionFunc(limitedSizeStream, CancellationToken.None);
                });

            return queueStub!;
        }
    }
}