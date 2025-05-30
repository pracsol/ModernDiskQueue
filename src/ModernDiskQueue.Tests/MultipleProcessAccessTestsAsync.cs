﻿// -----------------------------------------------------------------------
// <copyright file="MultipleProcessAccessTestsAsync.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

// ReSharper disable PossibleNullReferenceException
namespace ModernDiskQueue.Tests
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using NUnit.Framework;

    [TestFixture]
    public class MultipleProcessAccessTestsAsync : PersistentQueueTestsBase
    {
        private PersistentQueueFactory _factory;

        protected override string QueuePath => "./MultipleProcessAccessTests";

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
        [Description("Multiple PersistentQueue instances are " +
                     "pretty much the same as multiple processes to " +
                     "the DiskQueue library")]
        public void Can_access_from_multiple_queues_if_used_carefully()
        {
            var producerDone = new ManualResetEvent(false);
            var consumerDone = new ManualResetEvent(false);
            var received = new ConcurrentBag<byte[]>();
            int numberOfItems = 10;

            var t1 = new Thread(() =>
            {
                for (int i = 0; i < numberOfItems; i++)
                {
                    var task = AddToQueueAsync([1, 2, 3]);
                    task.Wait();
                }

                producerDone.Set();
            });
            var t2 = new Thread(() =>
            {
                while (received.Count < numberOfItems)
                {
                    var task = ReadQueueAsync();
                    task.Wait();
                    var data = task.Result;
                    if (data != null)
                    {
                        received.Add(data);
                    }
                }

                consumerDone.Set();
            });

            t1.Start();
            t2.Start();

            // Wait for producer to finish
            var producerFinished = producerDone.WaitOne(TimeSpan.FromSeconds(30));

            // Wait for consumer to finish
            var consumerFinished = consumerDone.WaitOne(TimeSpan.FromSeconds(25));

            if (!producerFinished)
            {
                Assert.Fail("Producer did not finish in time");
            }

            if (!consumerFinished)
            {
                Assert.Fail("Consumer did not finish in time");
            }

            Console.WriteLine($"Added {numberOfItems} items to queue.");
            Console.WriteLine($"Recieved {received.Count} items back.");
            Assert.That(received.Count, Is.EqualTo(numberOfItems), "Count of received items did not match number of items added.");
        }

        [Test]
        public async Task Can_access_from_multiple_queues_if_used_carefully_with_generic_container_and_serialisation()
        {
            var producerDone = new ManualResetEvent(false);
            var consumerDone = new ManualResetEvent(false);
            var received = new ConcurrentBag<string>();
            int numberOfItems = 10;
            Exception? lastException = null;

            var t1 = new Thread(() =>
            {
                try
                {
                    for (int i = 0; i < numberOfItems; i++)
                    {
                        var task = AddToQueueStringAsync("Hello");
                        task.Wait();
                    }

                    producerDone.Set();
                }
                catch (Exception ex)
                {
                    lastException = ex;
                    producerDone.Set();
                }
            });
            var t2 = new Thread(() =>
            {
                try
                {
                    while (received.Count < numberOfItems)
                    {
                        var task = ReadQueueStringAsync();
                        var data = task.Result;
                        if (data != null)
                        {
                            received.Add(data);
                        }
                    }

                    consumerDone.Set();
                }
                catch (Exception ex)
                {
                    lastException = ex;
                    consumerDone.Set();
                }
            });

            t1.Start();
            t2.Start();

            // Wait for producer to finish
            var producerFinished = producerDone.WaitOne(TimeSpan.FromSeconds(30));

            // Wait for consumer to finish
            var consumerFinished = consumerDone.WaitOne(TimeSpan.FromSeconds(25));

            if (!producerFinished)
            {
                Assert.Fail("Producer did not finish in time");
            }

            if (!consumerFinished)
            {
                Assert.Fail("Consumer did not finish in time");
            }

            if (lastException != null)
            {
                Assert.Fail($"Exception hit while trying to read queue: {lastException.Message}");
            }

            Assert.That(received.Count, Is.EqualTo(numberOfItems), "received items");
        }

        private async Task AddToQueueStringAsync(string data)
        {
            await Task.Delay(152);
            await using var queue = await _factory.WaitForAsync<string>(QueuePath, TimeSpan.FromSeconds(30));
            await using var session = await queue.OpenSessionAsync();

            await session.EnqueueAsync(data);
            await session.FlushAsync();
        }

        private async Task<string?> ReadQueueStringAsync()
        {
            string? data = null;
            try
            {
                await Task.Delay(121);
                await using var queue = await _factory.WaitForAsync<string>(QueuePath, TimeSpan.FromSeconds(30));
                await using var session = await queue.OpenSessionAsync();
                data = await session.DequeueAsync();
                await session.FlushAsync();
            }
            catch (Exception ex)
            {
                Assert.Fail($"Exception hit while trying to read queue: {ex.Message}");
            }

            return data;
        }

        private async Task AddToQueueAsync(byte[] data)
        {
            await Task.Delay(152);
            await using (var queue = await _factory.WaitForAsync(QueuePath, TimeSpan.FromSeconds(30)))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    await session.EnqueueAsync(data);
                    await session.FlushAsync();
                }
            }
        }

        private async Task<byte[]?> ReadQueueAsync()
        {
            await Task.Delay(121);
            await using (var queue = await _factory.WaitForAsync(QueuePath, TimeSpan.FromSeconds(30)))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    var data = await session.DequeueAsync();
                    await session.FlushAsync();
                    return data;
                }
            }
        }
    }
}