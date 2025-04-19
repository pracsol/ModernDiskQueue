namespace ModernDiskQueue.Tests
{
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

    [TestFixture, SingleThreaded]
    public class PersistentQueueSessionTestsAsync : PersistentQueueTestsBase
    {
        protected override string QueuePath => "./PersistentQueueSessionTest";

        [Test]
        public async Task Errors_raised_during_pending_write_will_be_thrown_on_flush()
        {
            var limitedSizeStream = new MemoryStream(new byte[4]);
            var fileStream = new FileStreamWrapper(limitedSizeStream);
            var queueStub = PersistentQueueWithMemoryStream(fileStream);

            try
            {
                using var session = new PersistentQueueSession(queueStub, fileStream, 1024 * 1024, 1000);
                await session.EnqueueAsync(new byte[64 * 1024 * 1024 + 1]);
                await session.FlushAsync();
                Assert.Fail("Expected PendingWriteException was not thrown.");
            }
            catch (AssertionException)
            {
                throw;
            }
            catch (PendingWriteException ex)
            {
                Assert.That(ex.Message, Is.EqualTo($"One or more errors occurred. (Error during pending writes: {Environment.NewLine} - Memory stream is not expandable.)"));
            }
            catch (Exception ex)
            {
                Assert.Fail($"Unexpected exception type: {ex.GetType().Name}: {ex.Message}");
            }
        }

        [Test]
        public async Task Errors_raised_during_flush_write_will_be_thrown_as_is()
        {
            var limitedSizeStream = new MemoryStream(new byte[4]);
            var fileStream = new FileStreamWrapper(limitedSizeStream);
            var queueStub = PersistentQueueWithMemoryStream(fileStream);

            try
            {
                PersistentQueueSession session = new PersistentQueueSession(queueStub, fileStream, 1024 * 1024, 1000);
                await using (session)
                {
                    await session.EnqueueAsync(new byte[64]);
                    await session.FlushAsync();
                }
            }
            catch (AssertionException)
            {
                throw;
            }
            catch (NotSupportedException ex)
            {
                Assert.That(ex.Message, Is.EqualTo(@"Memory stream is not expandable."), "Expected NotSupportedException was not thrown.");
            }
            catch (Exception ex)
            {
                Assert.Fail($"Unexpected exception type: {ex.GetType().Name}: {ex.Message}");
            }
            Assert.Fail("Expected NotSupportedException was not thrown.");
        }

        [Test]
        public async Task If_data_stream_is_truncated_will_raise_error()
        {
            await using (var queue = await PersistentQueue.CreateAsync(QueuePath))
            await using (var session = await queue.OpenSessionAsync())
            {
                await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                await session.FlushAsync();
            }
            using (var fs = new FileStream(System.IO.Path.Combine(QueuePath, "data.0"), FileMode.Open))
            {
                fs.SetLength(2);//corrupt the file
            }

            Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await using (var queue = await PersistentQueue.CreateAsync(QueuePath))
                {
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        await session.DequeueAsync();
                    }
                }
            });
        }

        [Test]
        public async Task If_data_stream_is_truncated_will_NOT_raise_error_if_truncated_entries_are_allowed_in_settings()
        {
            PersistentQueue.DefaultSettings.AllowTruncatedEntries = true;
            PersistentQueue.DefaultSettings.ParanoidFlushing = true;

            await using (var queue = await PersistentQueue.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                    await session.FlushAsync();
                }
            }
            using (var fs = new FileStream(System.IO.Path.Combine(QueuePath, "data.0"), FileMode.Open))
            {
                fs.SetLength(2);//corrupt the file
            }

            byte[]? bytes;
            await using (var queue = await PersistentQueue.CreateAsync(QueuePath))
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

            await using (var queue = await PersistentQueue.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                    await session.FlushAsync();
                }
            }
            using (var fs = new FileStream(System.IO.Path.Combine(QueuePath, "data.0"), FileMode.Open))
            {
                fs.SetLength(2);//corrupt the file
            }

            await using (var queue = await PersistentQueue.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    await session.EnqueueAsync(new byte[] { 5, 6, 7, 8 });
                    await session.FlushAsync();
                }
            }

            byte[]? bytes, corruptBytes;
            await using (var queue = await PersistentQueue.CreateAsync(QueuePath))
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

            queueStub.WhenForAnyArgs(x => x.AcquireWriter(default!, default!, default!))
                .Do(c => CallActionArgument(c, limitedSizeStream));

            try
            {
                queueStub.WhenForAnyArgs(x => x.AcquireWriterAsync(default!, default!, default!, default!))
                    .Do(async c =>
                    {
                        try
                        {
                            var actionFunc = c.Args()[1] as Func<IFileStream, Task<long>>;
                            await actionFunc!(limitedSizeStream);
                        }
                        catch (Exception)
                        {
                            throw;
                        }
                    });

            }
            catch (Exception)
            {
                throw;
            }

            return queueStub!;
        }

        // Update the helper method to properly handle async calls
        private static async Task CallActionArgument(CallInfo c, IFileStream ms)
        {
            var func = c.Args()[1] as Func<IFileStream, Task<long>>;
            await func!(ms);
        }
    }
}