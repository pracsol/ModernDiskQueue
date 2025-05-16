namespace ModernDiskQueue.Tests
{
    using Microsoft.Extensions.Logging;
    using NSubstitute;
    using NUnit.Framework;
    using System;
    using System.Threading.Tasks;

    [TestFixture]
    public class ParanoidFlushingTestsAsync : PersistentQueueTestsBase
    {
        protected override string QueuePath => "./ParanoidFlushingTests";

        private readonly byte[] _one = [1, 2, 3, 4];
        private readonly byte[] _two = [5, 6, 7, 8];

        private IPersistentQueueFactory  _factory = Substitute.For<IPersistentQueueFactory>();
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
        public async Task Paranoid_flushing_still_respects_session_rollback()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                // Clean up leftover data from previous failed test runs
                await queue.HardDeleteAsync(true);

                queue.Internals.ParanoidFlushing = true;

                // Flush only `_one`
                await using (var s1 = await queue.OpenSessionAsync())
                {
                    await s1.EnqueueAsync(_one);
                    await s1.FlushAsync();
                    // Without flush, _two will be rolled back.
                    await s1.EnqueueAsync(_two);
                }

                // Read without flushing
                await using (var s2 = await queue.OpenSessionAsync())
                {
                    var value = await s2.DequeueAsync();
                    Console.WriteLine($"First read from session 2: {BitConverter.ToString(value ?? [])}");
                    // Only _one should be on the queue, but definitely first read since we're FIFO.
                    Assert.That(value, Is.EquivalentTo(_one), "Unexpected item at head of queue");
                    // Should be nothing else on queue.
                    Assert.That(await s2.DequeueAsync(), Is.Null, "Too many items on queue");
                    // No flushing, so stuff stays on queue.
                }

                // Read again WITH flushing
                await using (var s3 = await queue.OpenSessionAsync())
                {
                    var value = await s3.DequeueAsync();
                    Console.WriteLine($"First read from session 3: {BitConverter.ToString(value ?? [])}");
                    // _one should still be on the queue, since we didn't flush.
                    Assert.That(value, Is.EquivalentTo(_one), "Queue was unexpectedly empty?");
                    // Should be nothing else on queue.
                    Assert.That(await s3.DequeueAsync(), Is.Null, "Too many items on queue");
                    await s3.FlushAsync();
                }

                // Read empty queue to be sure
                await using (var s4 = await queue.OpenSessionAsync())
                {
                    // Now queue should definitely be empty since we flushed on last read.
                    Assert.That(await s4.DequeueAsync(), Is.Null, "Queue was not empty after flush");
                    await s4.FlushAsync();
                }
            }
        }
    }
}