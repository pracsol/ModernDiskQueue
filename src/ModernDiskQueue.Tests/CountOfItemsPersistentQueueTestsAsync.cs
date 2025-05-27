// -----------------------------------------------------------------------
// <copyright file="CountOfItemsPersistentQueueTestsAsync.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

// ReSharper disable PossibleNullReferenceException
namespace ModernDiskQueue.Tests
{
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using NSubstitute;
    using NUnit.Framework;

    [TestFixture]
    [SingleThreaded]
    public class CountOfItemsPersistentQueueTestsAsync : PersistentQueueTestsBase
    {
        private IPersistentQueueFactory _factory = Substitute.For<IPersistentQueueFactory>();

        protected override string QueuePath => "./CountOfItemsTests";

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
        public async Task Can_get_count_from_queue()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                Assert.That(0, Is.EqualTo(await queue.GetEstimatedCountOfItemsInQueueAsync()));
            }
        }

        [Test]
        public async Task Can_enter_items_and_get_count_of_items()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                for (byte i = 0; i < 5; i++)
                {
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        await session.EnqueueAsync([i]);
                        await session.FlushAsync();
                    }
                }

                Assert.That(5, Is.EqualTo(await queue.GetEstimatedCountOfItemsInQueueAsync()));
            }
        }

        [Test]
        public async Task Can_get_count_of_items_after_queue_restart()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                for (byte i = 0; i < 5; i++)
                {
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        await session.EnqueueAsync(new[] { i });
                        await session.FlushAsync();
                    }
                }
            }

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                Assert.That(5, Is.EqualTo(await queue.GetEstimatedCountOfItemsInQueueAsync()));
            }
        }
    }
}