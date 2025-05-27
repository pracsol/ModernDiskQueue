// -----------------------------------------------------------------------
// <copyright file="MultiFileQueueTestsAsync.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

// ReSharper disable AssignNullToNotNullAttribute
namespace ModernDiskQueue.Tests
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using NSubstitute;
    using NUnit.Framework;

    [TestFixture]
    [SingleThreaded]
    public class MultiFileQueueTestsAsync : PersistentQueueTestsBase
    {
        private IPersistentQueueFactory _factory = Substitute.For<IPersistentQueueFactory>();

        protected override string QueuePath => "./MultiFileQueue";

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
        public async Task Can_limit_amount_of_items_in_queue_file()
        {
            await using (IPersistentQueue queue = await _factory.CreateAsync(QueuePath, 10))
            {
                Assert.That(10, Is.EqualTo(queue.MaxFileSize));
            }
        }

        [Test]
        public async Task Entering_more_than_count_of_items_will_work()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath, 10))
            {
                for (byte i = 0; i < 11; i++)
                {
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        await session.EnqueueAsync(new[] { i });
                        await session.FlushAsync();
                    }
                }

                Assert.That(11, Is.EqualTo(await queue.GetEstimatedCountOfItemsInQueueAsync()));
            }
        }

        [Test]
        public async Task When_creating_more_items_than_allowed_in_first_file_will_create_additional_file()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath, 10))
            {
                for (byte i = 0; i < 11; i++)
                {
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        await session.EnqueueAsync(new[] { i });
                        await session.FlushAsync();
                    }
                }

                Assert.That(1, Is.EqualTo(queue.Internals.CurrentFileNumber));
            }
        }

        [Test]
        public async Task Can_resume_writing_to_second_file_when_restart_queue()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath, 10))
            {
                for (byte i = 0; i < 11; i++)
                {
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        await session.EnqueueAsync([i]);
                        await session.FlushAsync();
                    }
                }

                Assert.That(1, Is.EqualTo(queue.Internals.CurrentFileNumber));
            }

            await using (var queue = await _factory.CreateAsync(QueuePath, 10))
            {
                for (byte i = 0; i < 2; i++)
                {
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        await session.EnqueueAsync([i]);
                        await session.FlushAsync();
                    }
                }

                Assert.That(1, Is.EqualTo(queue.Internals.CurrentFileNumber));
            }
        }

        [Test]
        public async Task Can_dequeue_from_all_files()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath, 10))
            {
                for (byte i = 0; i < 12; i++)
                {
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        await session.EnqueueAsync(new[] { i });
                        Console.WriteLine(queue.Internals.CurrentFileNumber);
                        await session.FlushAsync();
                    }
                }

                Assert.That(1, Is.EqualTo(queue.Internals.CurrentFileNumber));
                Console.WriteLine($"File number: {queue.Internals.CurrentFileNumber}");
            }

            await using (var queue = await _factory.CreateAsync(QueuePath, 10))
            {
                for (byte i = 0; i < 12; i++)
                {
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        var value = await session.DequeueAsync();
                        Console.WriteLine(value[0]);
                        Assert.That(value, Is.Not.Null);
                        Assert.That(i, Is.EqualTo(value?[0]));
                        await session.FlushAsync();
                    }
                }
            }
        }

        [Test]
        public async Task Can_dequeue_from_all_files_after_restart()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath, 10))
            {
                for (byte i = 0; i < 12; i++)
                {
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        await session.EnqueueAsync(new[] { i });
                        await session.FlushAsync();
                    }
                }

                Assert.That(1, Is.EqualTo(queue.Internals.CurrentFileNumber));
            }

            await using (var queue = await _factory.CreateAsync(QueuePath, 10))
            {
                for (byte i = 0; i < 3; i++)
                {
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        await session.EnqueueAsync(new[] { i });
                        await session.FlushAsync();
                    }
                }

                Assert.That(1, Is.EqualTo(queue.Internals.CurrentFileNumber));
            }

            await using (var queue = await _factory.CreateAsync(QueuePath, 10))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (byte i = 0; i < 12; i++)
                    {
                        var value = await session.DequeueAsync();
                        Assert.That(value, Is.Not.Null);
                        Assert.That(i, Is.EqualTo(value?[0]));
                        await session.FlushAsync();
                    }

                    for (byte i = 0; i < 3; i++)
                    {
                        var value = await session.DequeueAsync();
                        Assert.That(value, Is.Not.Null);
                        Assert.That(i, Is.EqualTo(value?[0]));
                        await session.FlushAsync();
                    }
                }
            }
        }

        [Test]
        public async Task After_reading_all_items_from_file_that_is_not_the_active_file_should_delete_file()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath, 10))
            {
                for (byte i = 0; i < 12; i++) // 12 individual bytes, and a 10 byte file limit
                {
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        await session.EnqueueAsync([i]);
                        await session.FlushAsync();
                    }
                }

                Assert.That(1, Is.EqualTo(queue.Internals.CurrentFileNumber));
            }

            await using (var queue = await _factory.CreateAsync(QueuePath, 10))
            {
                for (byte i = 0; i < 12; i++)
                {
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        var value = await session.DequeueAsync();
                        Assert.That(value, Is.Not.Null);
                        Assert.That(i, Is.EqualTo(value?[0]));
                        await session.FlushAsync();
                    }
                }
            }

            Assert.That(File.Exists(System.IO.Path.Combine(QueuePath, "data.0")), Is.False);
        }
    }
}