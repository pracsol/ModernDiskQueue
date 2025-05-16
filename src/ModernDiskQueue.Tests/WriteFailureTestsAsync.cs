
namespace ModernDiskQueue.Tests
{
    using Microsoft.Extensions.Logging;
    using ModernDiskQueue.Implementation;
    using NUnit.Framework;
    using System;
    using System.IO;
    using System.Threading.Tasks;

    [TestFixture]
    public class WriteFailureTestsAsync : PersistentQueueTestsBase
    {
        protected override string QueuePath => "./WriteFailureTests";

        private PersistentQueueFactory _factory = new ();
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
        public async Task EnqueueFailsIfDiskIsFullButDequeueStillWorks()
        {
            await using var subject = await _factory.CreateAsync(QueuePath);
            await subject.HardDeleteAsync(true);

            await using (var session = await subject.OpenSessionAsync())
            {
                for (int i = 0; i < 100; i++)
                {
                    await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                    await session.FlushAsync();
                }
            }

            await using (var session = await subject.OpenSessionAsync())
            {
                // Switch to a file system that fails on write.
                subject.Internals.SetFileDriver(new WriteFailureDriver());

                for (int i = 0; i < 3; i++)
                {
                    Console.WriteLine("Trying to dequeue using a file system that fails on write.");
                    var result = await session.DequeueAsync();
                    Console.WriteLine($"Value dequeued is {BitConverter.ToString(result ?? [])}");
                    Assert.That(result, Is.Not.Null);

                    Console.WriteLine("Now trying to enqueue, which should work before flushing.");
                    await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                    Console.WriteLine("Now trying to flush, which should fail with a file system designed to fail on write.");
                    try
                    {
                        await session.FlushAsync();
                    }
                    catch (IOException ex)
                    {
                        Console.WriteLine($"Expected exception: {ex.Message}");
                        Assert.Pass($"Caught expected exception when trying to write: {ex.GetType()} - {ex.Message} - {ex.StackTrace}");
                    }
                    catch (Exception ex)
                    {
                        Assert.Fail($"Unexpected exception type: {ex.GetType()} - {ex.Message}");
                    }
                    finally
                    {
                        // Restore driver so we can dispose correctly.
                        subject.Internals.SetFileDriver(new StandardFileDriver());
                    }
                }
            }

        }
    }
}