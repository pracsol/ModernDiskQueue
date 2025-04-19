using ModernDiskQueue.Implementation;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
// ReSharper disable PossibleNullReferenceException
// ReSharper disable AssignNullToNotNullAttribute

namespace ModernDiskQueue.Tests
{
    [TestFixture, SingleThreaded]
    public class TransactionLogTests : PersistentQueueTestsBase
    {
        protected override string QueuePath => "./TransactionLogTests";

        [Test]
        public void Transaction_log_size_shrink_after_queue_disposed()
        {
            long txSizeWhenOpen;
            var txLogInfo = new FileInfo(System.IO.Path.Combine(QueuePath, "transaction.log"));
            using (var queue = new PersistentQueue(QueuePath))
            {
                queue.Internals.ParanoidFlushing = false;
                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 10; j++)
                    {
                        session.Enqueue(Guid.NewGuid().ToByteArray());
                    }
                    session.Flush();
                }

                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 10; j++)
                    {
                        session.Dequeue();
                    }
                    session.Flush();
                }
                txSizeWhenOpen = txLogInfo.Length;
            }
            txLogInfo.Refresh();
            Assert.That(txLogInfo.Length, Is.LessThan(txSizeWhenOpen));
        }

        [Test]
        public void Count_of_items_will_remain_fixed_after_dequeueing_without_flushing()
        {
            using (var queue = new PersistentQueue(QueuePath))
            {
                queue.Internals.ParanoidFlushing = false;
                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 10; j++)
                    {
                        session.Enqueue(Guid.NewGuid().ToByteArray());
                    }
                    session.Flush();
                }

                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 10; j++)
                    {
                        session.Dequeue();
                    }
                    Assert.That(session.Dequeue(), Is.Null);

                    //	session.Flush(); explicitly removed
                }
            }
            using (var queue = new PersistentQueue(QueuePath))
            {
                Assert.That(10, Is.EqualTo(queue.EstimatedCountOfItemsInQueue));
            }
        }

        [Test]
        public void Dequeue_items_that_were_not_flushed_will_appear_after_queue_restart()
        {
            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 10; j++)
                    {
                        session.Enqueue(Guid.NewGuid().ToByteArray());
                    }
                    session.Flush();
                }

                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 10; j++)
                    {
                        session.Dequeue();
                    }
                    Assert.That(session.Dequeue(), Is.Null);

                    //	session.Flush(); explicitly removed
                }
            }
            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 10; j++)
                    {
                        session.Dequeue();
                    }
                    Assert.That(session.Dequeue(), Is.Null);
                    session.Flush();
                }
            }
        }

        [Test]
        public void If_tx_log_grows_too_large_it_will_be_trimmed_while_queue_is_in_operation()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(QueuePath, "transaction.log"));

            using var queue = new PersistentQueue(QueuePath);
            queue.SuggestedMaxTransactionLogSize = 32; // single entry
            queue.Internals.ParanoidFlushing = false;

            using (var session = queue.OpenSession())
            {
                for (int j = 0; j < 20; j++)
                {
                    session.Enqueue(Guid.NewGuid().ToByteArray());
                }
                session.Flush();
            }
            // there is no way optimize here, so we should get expected size, even though it is bigger than
            // what we suggested as the max
            txLogInfo.Refresh();
            long txSizeWhenOpen = txLogInfo.Length;

            using (var session = queue.OpenSession())
            {
                for (int j = 0; j < 20; j++)
                {
                    session.Dequeue();
                }
                Assert.That(session.Dequeue(), Is.Null);

                session.Flush();
            }
            txLogInfo.Refresh();
            Assert.That(txLogInfo.Length, Is.LessThan(txSizeWhenOpen));
        }

        [Test]
        public void Truncated_transaction_is_ignored_with_default_settings()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(QueuePath, "transaction.log"));

            using (var queue = new PersistentQueue(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                queue.Internals.ParanoidFlushing = false;

                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 20; j++)
                    {
                        session.Enqueue(BitConverter.GetBytes(j));
                        session.Flush();
                    }
                }
            }

            using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                txLog.SetLength(txLog.Length - 5);// corrupt last transaction
                txLog.Flush();
            }

            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 19; j++)
                    {
                        var bytes = session.Dequeue() ?? throw new Exception("read failed");
                        Assert.That(j, Is.EqualTo(BitConverter.ToInt32(bytes, 0)));
                    }
                    Assert.That(session.Dequeue(), Is.Null);// the last transaction was corrupted
                    session.Flush();
                }
            }
        }

        [Test]
        public void Can_handle_truncated_start_transaction_separator()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(QueuePath, "transaction.log"));

            using (var queue = new PersistentQueue(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 20; j++)
                    {
                        session.Enqueue(BitConverter.GetBytes(j));
                        session.Flush();
                    }
                }
            }

            using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                txLog.SetLength(5); // truncate log to halfway through start marker
                txLog.Flush();
            }

            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    Assert.That(session.Dequeue(), Is.Null);// the last transaction was corrupted
                    session.Flush();
                }
            }
        }

        [Test]
        public void Can_handle_truncated_data()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(QueuePath, "transaction.log"));

            using (var queue = new PersistentQueue(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 20; j++)
                    {
                        session.Enqueue(BitConverter.GetBytes(j));
                        session.Flush();
                    }
                }
            }

            using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                txLog.SetLength(100); // truncate log to halfway through log entry
                txLog.Flush();
            }

            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    Assert.That(session.Dequeue(), Is.Null);// the last transaction was corrupted
                    session.Flush();
                }
            }
        }

        [Test]
        public void Can_handle_truncated_end_transaction_separator()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(QueuePath, "transaction.log"));

            using (var queue = new PersistentQueue(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 20; j++)
                    {
                        session.Enqueue(BitConverter.GetBytes(j));
                        session.Flush();
                    }
                }
            }

            using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                txLog.SetLength(368); // truncate end transaction marker
                txLog.Flush();
            }

            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    Assert.That(session.Dequeue(), Is.Null);// the last transaction was corrupted
                    session.Flush();
                }
            }
        }

        [Test]
        public void Can_handle_transaction_with_only_zero_length_entries()
        {
            using (var queue = new PersistentQueue(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 20; j++)
                    {
                        session.Enqueue([]);
                        session.Flush();
                    }
                }
            }

            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 20; j++)
                    {
                        Assert.That(session.Dequeue(), Is.Empty);
                    }
                    Assert.That(session.Dequeue(), Is.Null);
                    session.Flush();
                }
            }
        }

        [Test]
        public void Can_handle_end_separator_used_as_data()
        {
            using (var queue = new PersistentQueue(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 20; j++)
                    {
                        session.Enqueue(Constants.EndTransactionSeparator); // ???
                        session.Flush();
                    }
                    session.Flush();
                }
            }

            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    Assert.That(Constants.EndTransactionSeparator, Is.EqualTo(session.Dequeue()));
                    session.Flush();
                }
            }
        }

        [Test]
        public void Can_handle_start_separator_used_as_data()
        {
            using (var queue = new PersistentQueue(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 20; j++)
                    {
                        session.Enqueue(Constants.StartTransactionSeparator); // ???
                        session.Flush();
                    }
                    session.Flush();
                }
            }

            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    Assert.That(Constants.StartTransactionSeparator, Is.EqualTo(session.Dequeue()));
                    session.Flush();
                }
            }
        }

        [Test]
        public void Can_handle_zero_length_entries_at_start()
        {
            using (var queue = new PersistentQueue(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                using (var session = queue.OpenSession())
                {
                    Console.WriteLine("Enqueueing zero length entry.");
                    session.Enqueue([]);
                    session.Flush();
                    Console.WriteLine("Enqueueing real data entries.");
                    for (int j = 0; j < 19; j++)
                    {
                        session.Enqueue([1]);
                        session.Flush();
                    }
                }
            }

            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    Console.WriteLine("Dequeueing entries, FIFO should hit zero length first.");
                    for (int j = 0; j < 20; j++)
                    {
                        Console.WriteLine($"Dequeue entry {j}.");
                        Assert.That(session.Dequeue(), Is.Not.Null);
                        session.Flush();
                    }
                }
            }
        }

        [Test]
        public void Can_handle_zero_length_entries_at_end()
        {
            using (var queue = new PersistentQueue(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 19; j++)
                    {
                        session.Enqueue([1]);
                        session.Flush();
                    }
                    session.Enqueue([]);
                    session.Flush();
                }
            }

            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 20; j++)
                    {
                        Assert.That(session.Dequeue(), Is.Not.Null);
                        session.Flush();
                    }
                }
            }
        }

        [Test]
        public void Can_restore_data_when_a_transaction_set_is_partially_truncated()
        {
            PersistentQueue.DefaultSettings.AllowTruncatedEntries = false;
            var txLogInfo = new FileInfo(System.IO.Path.Combine(QueuePath, "transaction.log"));
            using (var queue = new PersistentQueue(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 5; j++)
                    {
                        session.Enqueue([(byte)(j + 1)]);
                    }
                    session.Flush();
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

            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 5; j++)
                    {
                        Assert.That(session.Dequeue(), Is.EquivalentTo([(byte)(j + 1)]));
                        session.Flush();
                    }
                    for (int j = 0; j < 5; j++)
                    {
                        Assert.That(session.Dequeue(), Is.EquivalentTo([(byte)(j + 1)]));
                        session.Flush();
                    }

                    Assert.That(session.Dequeue(), Is.Null);
                    session.Flush();
                }
            }
        }

        [Test]
        public void Can_restore_data_when_a_transaction_set_is_partially_overwritten_when_throwOnConflict_is_false()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(QueuePath, "transaction.log"));
            using (var queue = new PersistentQueue(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 5; j++)
                    {
                        session.Enqueue([]);
                    }
                    session.Flush();
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
                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 5; j++) // first 5 should be OK
                    {
                        Assert.That(session.Dequeue(), Is.Not.Null);
                    }
                    Assert.That(session.Dequeue(), Is.Null); // duplicated 5 should be silently lost.
                    session.Flush();
                }
            }
        }

        [Test]
        public void Will_remove_truncated_transaction()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(QueuePath, "transaction.log"));

            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 20; j++)
                    {
                        session.Enqueue(BitConverter.GetBytes(j));
                        session.Flush();
                    }
                }
            }

            using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                txLog.SetLength(5);// corrupt all transactions
                txLog.Flush();
            }

            new PersistentQueue(QueuePath).Dispose();

            txLogInfo.Refresh();

            Assert.That(36, Is.EqualTo(txLogInfo.Length));//empty transaction size
        }

        [Test]
        public void Truncated_transaction_is_ignored_and_can_continue_to_add_items_to_queue()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(QueuePath, "transaction.log"));

            using (var queue = new PersistentQueue(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                queue.Internals.ParanoidFlushing = false;
                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 20; j++)
                    {
                        session.Enqueue(BitConverter.GetBytes(j));
                        session.Flush();
                    }
                }
            }

            using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                txLog.SetLength(txLog.Length - 5);// corrupt last transaction
                txLog.Flush();
            }

            using (var queue = new PersistentQueue(QueuePath))
            {
                // avoid auto tx log trimming
                queue.TrimTransactionLogOnDispose = false;
                using (var session = queue.OpenSession())
                {
                    for (int j = 20; j < 40; j++)
                    {
                        session.Enqueue(BitConverter.GetBytes(j));
                    }
                    session.Flush();
                }
            }

            var data = new List<int>();
            using (var queue = new PersistentQueue(QueuePath))
            {
                using (var session = queue.OpenSession())
                {
                    var dequeue = session.Dequeue();
                    while (dequeue != null)
                    {
                        data.Add(BitConverter.ToInt32(dequeue, 0));
                        dequeue = session.Dequeue();
                    }
                    session.Flush();
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