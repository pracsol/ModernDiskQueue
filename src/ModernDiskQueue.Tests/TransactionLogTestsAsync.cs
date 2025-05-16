namespace ModernDiskQueue.Tests
{
    using Microsoft.Extensions.Logging;
    using ModernDiskQueue.Implementation;
    using NSubstitute;
    using NUnit.Framework;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading.Tasks;

    [TestFixture, SingleThreaded]
    public class TransactionLogTestsAsync : PersistentQueueTestsBase
    {
        protected override string QueuePath => "./TransactionLogTests";

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
        public async Task Transaction_log_size_shrink_after_queue_disposed()
        {
            long txSizeWhenOpen;
            var txLogInfo = new FileInfo(System.IO.Path.Combine(QueuePath, "transaction.log"));
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                queue.Internals.ParanoidFlushing = false;
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 10; j++)
                    {
                        await session.EnqueueAsync(Guid.NewGuid().ToByteArray());
                    }
                    await session.FlushAsync();
                }

                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 10; j++)
                    {
                        await session.DequeueAsync();
                    }
                    await session.FlushAsync();
                }
                txSizeWhenOpen = txLogInfo.Length;
            }
            txLogInfo.Refresh();
            Assert.That(txLogInfo.Length, Is.LessThan(txSizeWhenOpen));
        }

        [Test]
        public async Task Count_of_items_will_remain_fixed_after_dequeueing_without_flushing()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                queue.Internals.ParanoidFlushing = false;
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 10; j++)
                    {
                        await session.EnqueueAsync(Guid.NewGuid().ToByteArray());
                    }
                    await session.FlushAsync();
                }

                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 10; j++)
                    {
                        await session.DequeueAsync();
                    }
                    Assert.That(await session.DequeueAsync(), Is.Null);

                    //	await session.FlushAsync(); explicitly removed so no dequeues get committed.
                }
            }
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                int finalCountOfItems = await queue.GetEstimatedCountOfItemsInQueueAsync();
                Assert.That(10, Is.EqualTo(finalCountOfItems));
            }
        }

        [Test]
        public async Task Dequeue_items_that_were_not_flushed_will_appear_after_queue_restart()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 10; j++)
                    {
                        await session.EnqueueAsync(Guid.NewGuid().ToByteArray());
                    }
                    await session.FlushAsync();
                }

                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 10; j++)
                    {
                        await session.DequeueAsync();
                    }
                    Assert.That(await session.DequeueAsync(), Is.Null);

                    //	await session.FlushAsync(); explicitly removed so dequeued items not removed.
                }
            }
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 10; j++)
                    {
                        await session.DequeueAsync();
                    }
                    Assert.That(await session.DequeueAsync(), Is.Null);
                    await session.FlushAsync();
                }
            }
        }

        [Test]
        public async Task If_tx_log_grows_too_large_it_will_be_trimmed_while_queue_is_in_operation()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(QueuePath, "transaction.log"));

            await using var queue = await _factory.CreateAsync(QueuePath);
            queue.SuggestedMaxTransactionLogSize = 32; // single entry
            queue.Internals.ParanoidFlushing = false;

            await using (var session = await queue.OpenSessionAsync())
            {
                for (int j = 0; j < 20; j++)
                {
                    await session.EnqueueAsync(Guid.NewGuid().ToByteArray());
                }
                await session.FlushAsync();
            }
            // there is no way optimize here, so we should get expected size, even though it is bigger than
            // what we suggested as the max
            txLogInfo.Refresh();
            long txSizeWhenOpen = txLogInfo.Length;
            Console.WriteLine($"Size of transaction.log after queue full: {txSizeWhenOpen}");

            await using (var session = await queue.OpenSessionAsync())
            {
                for (int j = 0; j < 20; j++)
                {
                    await session.DequeueAsync();
                }
                Assert.That(await session.DequeueAsync(), Is.Null);

                await session.FlushAsync();
            }
            txLogInfo.Refresh();
            Console.WriteLine($"Size of transaction.log after queue empty: {txSizeWhenOpen}");
            Assert.That(txLogInfo.Length, Is.LessThan(txSizeWhenOpen));
        }

        [Test]
        public async Task Truncated_transaction_is_ignored_with_default_settings()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(QueuePath, "transaction.log"));

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                queue.Internals.ParanoidFlushing = false;

                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 20; j++)
                    {
                        await session.EnqueueAsync(BitConverter.GetBytes(j));
                        await session.FlushAsync();
                    }
                }
            }

            using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                txLog.SetLength(txLog.Length - 5);// corrupt last transaction
                txLog.Flush();
            }

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 19; j++)
                    {
                        var bytes = await session.DequeueAsync() ?? throw new Exception("read failed");
                        Assert.That(j, Is.EqualTo(BitConverter.ToInt32(bytes, 0)));
                    }
                    Assert.That(await session.DequeueAsync(), Is.Null);// the last transaction was corrupted
                    await session.FlushAsync();
                }
            }
        }

        [Test]
        public async Task Can_handle_truncated_start_transaction_separator()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(QueuePath, "transaction.log"));

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 20; j++)
                    {
                        await session.EnqueueAsync(BitConverter.GetBytes(j));
                        await session.FlushAsync();
                    }
                }
            }

            using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                txLog.SetLength(5); // truncate log to halfway through start marker
                txLog.Flush();
            }

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    Assert.That(await session.DequeueAsync(), Is.Null);// the last transaction was corrupted
                    await session.FlushAsync();
                }
            }
        }

        [Test]
        public async Task Can_handle_truncated_data()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(QueuePath, "transaction.log"));

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 20; j++)
                    {
                        await session.EnqueueAsync(BitConverter.GetBytes(j));
                        await session.FlushAsync();
                    }
                }
            }

            using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                txLog.SetLength(100); // truncate log to halfway through log entry
                txLog.Flush();
            }

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    Assert.That(await session.DequeueAsync(), Is.Null);// the last transaction was corrupted
                    await session.FlushAsync();
                }
            }
        }

        [Test]
        public async Task Can_handle_truncated_end_transaction_separator()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(QueuePath, "transaction.log"));

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 20; j++)
                    {
                        await session.EnqueueAsync(BitConverter.GetBytes(j));
                        await session.FlushAsync();
                    }
                }
            }

            using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                txLog.SetLength(368); // truncate end transaction marker
                txLog.Flush();
            }

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    Assert.That(await session.DequeueAsync(), Is.Null);// the last transaction was corrupted
                    await session.FlushAsync();
                }
            }
        }



        [Test]
        public async Task Can_handle_transaction_with_only_zero_length_entries()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 20; j++)
                    {
                        await session.EnqueueAsync([]);
                        await session.FlushAsync();
                    }
                }
            }

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 20; j++)
                    {
                        Assert.That(await session.DequeueAsync(), Is.Empty);
                    }
                    Assert.That(await session.DequeueAsync(), Is.Null);
                    await session.FlushAsync();
                }
            }
        }

        [Test]
        public async Task Can_handle_end_separator_used_as_data()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 20; j++)
                    {
                        await session.EnqueueAsync(Constants.EndTransactionSeparator); // ???
                        await session.FlushAsync();
                    }
                    await session.FlushAsync();
                }
            }

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    Assert.That(Constants.EndTransactionSeparator, Is.EqualTo(await session.DequeueAsync()));
                    await session.FlushAsync();
                }
            }
        }

        [Test]
        public async Task Can_handle_start_separator_used_as_data()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 20; j++)
                    {
                        await session.EnqueueAsync(Constants.StartTransactionSeparator); // ???
                        await session.FlushAsync();
                    }
                    await session.FlushAsync();
                }
            }

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    Assert.That(Constants.StartTransactionSeparator, Is.EqualTo(await session.DequeueAsync()));
                    await session.FlushAsync();
                }
            }
        }

        [Test]
        public async Task Can_handle_zero_length_entries_at_start()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                await using (var session = await queue.OpenSessionAsync())
                {
                    Console.WriteLine("Enqueueing zero length entry.");
                    await session.EnqueueAsync([]);
                    await session.FlushAsync();
                    Console.WriteLine("Enqueueing real data entries.");
                    for (int j = 0; j < 19; j++)
                    {
                        await session.EnqueueAsync([1]);
                        await session.FlushAsync();
                    }
                }
            }

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    Console.WriteLine("Dequeueing entries, FIFO should hit zero length first.");
                    for (int j = 0; j < 20; j++)
                    {
                        Console.WriteLine($"Dequeue entry {j}.");
                        var entry = await session.DequeueAsync();
                        Console.WriteLine($"Entry data: {entry}");
                        Assert.That(entry, Is.Not.Null);
                        await session.FlushAsync();
                    }
                }
            }
        }

        [Test]
        public async Task Can_handle_zero_length_entries_at_end()
        {
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 19; j++)
                    {
                        await session.EnqueueAsync([1]);
                        await session.FlushAsync();
                    }
                    await session.EnqueueAsync([]);
                    await session.FlushAsync();
                }
            }

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 20; j++)
                    {
                        Assert.That(await session.DequeueAsync(), Is.Not.Null);
                        await session.FlushAsync();
                    }
                }
            }
        }

        [Test]
        public async Task Can_restore_data_when_a_transaction_set_is_partially_truncated()
        {
            PersistentQueue.DefaultSettings.AllowTruncatedEntries = false;
            var txLogInfo = new FileInfo(System.IO.Path.Combine(QueuePath, "transaction.log"));
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 5; j++)
                    {
                        await session.EnqueueAsync([(byte)(j + 1)]);
                    }
                    await session.FlushAsync();
                }
            }

            using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                var buf = new byte[(int)txLog.Length];
                var actual = txLog.Read(buf, 0, (int)txLog.Length);
                Assert.That(txLog.Length, Is.EqualTo(actual));
                txLog.Write(buf, 0, buf.Length);        // a 'good' extra session
                txLog.Write(buf, 0, buf.Length / 2);    // a 'bad' extra session
                txLog.Flush();
            }

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 5; j++)
                    {
                        Assert.That(await session.DequeueAsync(), Is.EquivalentTo([(byte)(j + 1)]));
                        await session.FlushAsync();
                    }
                    for (int j = 0; j < 5; j++)
                    {
                        Assert.That(await session.DequeueAsync(), Is.EquivalentTo([(byte)(j + 1)]));
                        await session.FlushAsync();
                    }

                    Assert.That(await session.DequeueAsync(), Is.Null);
                    await session.FlushAsync();
                }
            }
        }

        [Test]
        public async Task Can_restore_data_when_a_transaction_set_is_partially_overwritten_when_throwOnConflict_is_false()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(QueuePath, "transaction.log"));
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 5; j++)
                    {
                        await session.EnqueueAsync([]);
                    }
                    await session.FlushAsync();
                }
            }

            using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                var buf = new byte[(int)txLog.Length];
                var actual = txLog.Read(buf, 0, (int)txLog.Length);
                Assert.That(txLog.Length, Is.EqualTo(actual));
                txLog.Write(buf, 0, buf.Length - 16); // new session, but with missing end marker
                txLog.Write(Constants.StartTransactionSeparator, 0, 16);
                txLog.Flush();
            }

            using (var queue = new PersistentQueue(QueuePath, Constants._32Megabytes, throwOnConflict: false))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 5; j++) // first 5 should be OK
                    {
                        Assert.That(await session.DequeueAsync(), Is.Not.Null);
                    }
                    Assert.That(await session.DequeueAsync(), Is.Null); // duplicated 5 should be silently lost.
                    await session.FlushAsync();
                }
            }
        }

        [Test]
        public async Task Will_remove_truncated_transaction()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(QueuePath, "transaction.log"));

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 20; j++)
                    {
                        await session.EnqueueAsync(BitConverter.GetBytes(j));
                        await session.FlushAsync();
                    }
                }
            }
            txLogInfo.Refresh();
            Console.WriteLine($"Size of full transaction.log: {txLogInfo.Length}");

            using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                txLog.SetLength(5);// corrupt all transactions
                txLog.Flush();
                txLogInfo.Refresh();
                Console.WriteLine($"Size of corrupt transaction.log: {txLogInfo.Length}");
            }

            await (await _factory.CreateAsync(QueuePath)).DisposeAsync();

            txLogInfo.Refresh();
            Console.WriteLine($"Size of reset transaction.log: {txLogInfo.Length}");
            Assert.That(36, Is.EqualTo(txLogInfo.Length)); //empty transaction size
        }

        [Test]
        public async Task Truncated_transaction_is_ignored_and_can_continue_to_add_items_to_queue()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(QueuePath, "transaction.log"));

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                queue.Internals.ParanoidFlushing = false;
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 0; j < 20; j++)
                    {
                        await session.EnqueueAsync(BitConverter.GetBytes(j));
                        await session.FlushAsync();
                    }
                }
            }

            using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                txLog.SetLength(txLog.Length - 5);// corrupt last transaction
                txLog.Flush();
            }

            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                await using (var session = await queue.OpenSessionAsync())
                {
                    for (int j = 20; j < 40; j++)
                    {
                        await session.EnqueueAsync(BitConverter.GetBytes(j));
                    }
                    await session.FlushAsync();
                }
            }

            var data = new List<int>();
            await using (var queue = await _factory.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    var dequeue = await session.DequeueAsync();
                    while (dequeue != null)
                    {
                        data.Add(BitConverter.ToInt32(dequeue, 0));
                        dequeue = await session.DequeueAsync();
                    }
                    await session.FlushAsync();
                }
            }
            var expected = 0;
            foreach (var i in data)
            {
                if (expected == 19)
                    continue;
                Assert.That(expected, Is.EqualTo(data[i]));
                expected++;
            }
        }
    }
}