// -----------------------------------------------------------------------
// <copyright file="WriteFailureTestsAsync.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Tests
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using ModernDiskQueue.Implementation;
    using NUnit.Framework;

    [TestFixture]
    public class WriteFailureTestsAsync : PersistentQueueTestsBase
    {
        private PersistentQueueFactory _factory = new();

        protected override string QueuePath => "./WriteFailureTests";

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