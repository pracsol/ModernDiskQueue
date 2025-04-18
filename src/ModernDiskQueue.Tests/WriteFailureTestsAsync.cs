
namespace ModernDiskQueue.Tests
{
    using System.IO;
    using ModernDiskQueue.Implementation;
    using NUnit.Framework;
    using System.Threading.Tasks;

    [TestFixture]
    public class WriteFailureTestsAsync : PersistentQueueTestsBase
    {
        protected override string QueuePath => "./WriteFailureTests";

        [Test]
        public async Task EnqueueFailsIfDiskIsFullButDequeueStillWorks()
        {
            await using var subject = await PersistentQueue.CreateAsync(QueuePath);
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
                    var result = await session.DequeueAsync();
                    Assert.That(result, Is.Not.Null);

                    await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                    Assert.Throws<IOException>(async () => { await session.FlushAsync(); }, "should have thrown an exception when trying to write");
                }
            }
            
            // Restore driver so we can dispose correctly.
            subject.Internals.SetFileDriver(new StandardFileDriver());
        }
    }
}