namespace ModernDiskQueue.Tests
{
    using NUnit.Framework;
    using System.Threading.Tasks;

    [TestFixture]
    public class ParanoidFlushingTestsAsync : PersistentQueueTestsBase
    {
        protected override string QueuePath => "./ParanoidFlushingTests";

        private readonly byte[] _one = { 1, 2, 3, 4 };
        private readonly byte[] _two = { 5, 6, 7, 8 };

        [Test]
        public async Task Paranoid_flushing_still_respects_session_rollback()
        {
            using (var queue = await PersistentQueue.CreateAsync(QueuePath))
            {
                // Clean up leftover data from previous failed test runs
                await queue.HardDeleteAsync(true);

                queue.Internals.ParanoidFlushing = true;

                // Flush only `_one`
                using (var s1 = await queue.OpenSessionAsync())
                {
                    await s1.EnqueueAsync(_one);
                    await s1.EnqueueAsync(_two);
                    await s1.FlushAsync();
                }

                // Read without flushing
                using (var s2 = await queue.OpenSessionAsync())
                {
                    Assert.That(await s2.DequeueAsync(), Is.EquivalentTo(_one), "Unexpected item at head of queue");
                    Assert.That(await s2.DequeueAsync(), Is.Null, "Too many items on queue");
                }

                // Read again WITH flushing
                using (var s3 = await queue.OpenSessionAsync())
                {
                    Assert.That(await s3.DequeueAsync(), Is.EquivalentTo(_one), "Queue was unexpectedly empty?");
                    Assert.That(await s3.DequeueAsync(), Is.Null, "Too many items on queue");
                    await s3.FlushAsync();
                }

                // Read empty queue to be sure
                using (var s4 = await queue.OpenSessionAsync())
                {
                    Assert.That(await s4.DequeueAsync(), Is.Null, "Queue was not empty after flush");
                    await s4.FlushAsync();
                }
            }
        }
    }
}