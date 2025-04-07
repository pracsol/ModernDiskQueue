using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

// ReSharper disable PossibleNullReferenceException
// ReSharper disable AssignNullToNotNullAttribute

namespace ModernDiskQueue.Tests
{
	[TestFixture, Explicit, SingleThreaded]
	public class PerformanceTests : PersistentQueueTestsBase
	{
		protected override string Path => "PerformanceTests";

		[Test, Description(
			"With a mid-range SSD, this is some 20x slower " +
			"than with a single flush (depends on disk speed)")]
		public void Enqueue_million_items_with_100_flushes()
		{
			using (var queue = new PersistentQueue(Path))
			{
				for (int i = 0; i < 100; i++)
				{
					using (var session = queue.OpenSession())
					{
						for (int j = 0; j < 10000; j++)
						{
							session.Enqueue(Guid.NewGuid().ToByteArray());
						}
						session.Flush();
					}
				}
			}
		}

		[Test]
		public void Enqueue_million_items_with_single_flush()
		{
			using (var queue = new PersistentQueue(Path))
			{
				using (var session = queue.OpenSession())
				{
					for (int i = 0; i < LargeCount; i++)
					{
						session.Enqueue(Guid.NewGuid().ToByteArray());
					}
					session.Flush();
				}
			}
		}

		[Test]
		public void write_heavy_multi_thread_workload()
		{
			using (var queue = new PersistentQueue(Path)) { queue.HardDelete(false); }

			var rnd = new Random();
			var threads = new Thread[200];
			
			// enqueue threads
			for (int i = 0; i < 100; i++)
			{
				var j = i;
				threads[i] = new Thread(() => {
					for (int k = 0; k < 10; k++)
					{
						Thread.Sleep(rnd.Next(5));
						using (var q = PersistentQueue.WaitFor(Path, TimeSpan.FromSeconds(50)))
						{
							using var s = q.OpenSession();
							s.Enqueue(Encoding.ASCII.GetBytes($"Thread {j} enqueue {k}"));
							s.Flush();
						}
					}
				}){IsBackground = true};
				threads[i].Start();
			}
			
			// dequeue single
			Thread.Sleep(1000);
			var count = 0;
			while (true)
			{
				byte[]? bytes;
				using (var q = PersistentQueue.WaitFor(Path, TimeSpan.FromSeconds(50)))
				{
					using var s = q.OpenSession();

					bytes = s.Dequeue();
					s.Flush();
				}

				if (bytes is null) break;
				count++;
				Console.WriteLine(Encoding.ASCII.GetString(bytes));
			}
			Assert.That(count, Is.EqualTo(1000), "did not receive all messages");
		}

        /// <summary>
        /// This test simulates a read-heavy workload with multiple threads concurrenty dequeuing items from the queue.
        /// </summary>
        [Test]
		public void read_heavy_multi_thread_workload()
		{
			DateTime testStartTime = DateTime.Now;
			using (var queue = new PersistentQueue(Path)) { queue.HardDelete(false); }
			// shared counter for total dequeues
			int totalDequeues = 0;

			// enqueue 1000 items in a single thread.
			var enqueueThread = new Thread(() =>
			{
				var enqueueStartTime = DateTime.Now;
				for (int i = 0; i < 1000; i++)
				{
					using var q = PersistentQueue.WaitFor(Path, TimeSpan.FromSeconds(50));
					using var s = q.OpenSession();
					s.Enqueue(Encoding.ASCII.GetBytes($"Enqueued item {i}"));
					s.Flush();
				}
                Console.WriteLine($"Enqueue thread finished, took {(DateTime.Now - enqueueStartTime).TotalSeconds} seconds.");
            });

			enqueueThread.Start();
			//enqueueThread.Join(); // wait for the enqueue thread to finish
			// If we don't wait for the enqueue thread to complete, the dequeue thread will start over top of it. 
			// The dequeue threads will quickly outpace the writing (enqueue) thread if enough head start isn't
			// given (thread.sleep). This also depends greatly on disk performance. Instead of doing a single flush
			// on the writes, a flush per enqueue is going to be very slow. But if this test is to simulate
			// a very active concurrent environment with more readers than writers, this may be a good way to 
			// understand the performance limitations and characteristics.

            Thread.Sleep(18000);
			var rnd = new Random();
			var threads = new Thread[200];

			DateTime dequeueStartTime = DateTime.Now;

			try
			{
				// dequeue threads
				for (int i = 0; i < 100; i++)
				{
					threads[i] = new Thread(() =>
					{
                        var count = 10;
						while (count > 0)
						{
							Thread.Sleep(rnd.Next(5));
							using (var q = PersistentQueue.WaitFor(Path, TimeSpan.FromSeconds(80)))
							{
								using var s = q.OpenSession();
								var data = s.Dequeue();
								if (data != null)
								{
									count--;
                                    int newCount = Interlocked.Increment(ref totalDequeues);
                                    //Console.WriteLine($"Thread {Thread.CurrentThread.ManagedThreadId} dequeued: {Encoding.ASCII.GetString(data)}, Total: {newCount}");
                                }

								s.Flush();
							}
						}
					})
					{ IsBackground = true };
					threads[i].Start();
				}

				for (int e = 0; e < 100; e++)
				{
					if (!threads[e].Join(80_000)) Assert.Fail($"reader timeout on thread {e}");
				}
            }
			catch (SuccessException) { }
			catch (Exception ex)
			{
				Assert.Fail($"Exception during read-heavy workload: {ex.GetType().Name} {ex.Message} {ex.StackTrace}; Dequeue counter was at: {totalDequeues}.");
            }
			Console.WriteLine($"All dequeue threads finished, took {(DateTime.Now - dequeueStartTime).TotalSeconds} seconds. Total dequeues: {totalDequeues}.");
            Console.WriteLine($"Total test time took {(DateTime.Now - testStartTime).TotalSeconds} seconds.");
        }

		[Test]
		public void Enqueue_and_dequeue_million_items_same_queue()
		{
			using (var queue = new PersistentQueue(Path))
			{
				using (var session = queue.OpenSession())
				{
					for (int i = 0; i < LargeCount; i++)
					{
						session.Enqueue(Guid.NewGuid().ToByteArray());
					}
					session.Flush();
				}
			
				using (var session = queue.OpenSession())
				{
					for (int i = 0; i < LargeCount; i++)
					{
						Ignore();
					}
					session.Flush();
				}
			}
		}

		private static void Ignore() { }

		[Test]
		public void Enqueue_and_dequeue_million_items_restart_queue()
		{
			using (var queue = new PersistentQueue(Path))
			{
				using (var session = queue.OpenSession())
				{
					for (int i = 0; i < LargeCount; i++)
					{
						session.Enqueue(Guid.NewGuid().ToByteArray());
					}
					session.Flush();
				}
			}

			using (var queue = new PersistentQueue(Path))
			{
				using (var session = queue.OpenSession())
				{
					for (int i = 0; i < LargeCount; i++)
					{
						Ignore();
					}
					session.Flush();
				}
			}
		}

		[Test]
		public void Enqueue_and_dequeue_large_items_with_restart_queue()
		{
			var random = new Random();
			var itemsSizes = new List<int>();
			using (var queue = new PersistentQueue(Path))
			{
				using (var session = queue.OpenSession())
				{
					for (int i = 0; i < SmallCount; i++)
					{
						var data = new byte[random.Next(1024 * 512, 1024 * 1024)];
						itemsSizes.Add(data.Length);
						session.Enqueue(data);
					}

					session.Flush();
				}
			}

			using (var queue = new PersistentQueue(Path))
			{
				using (var session = queue.OpenSession())
				{
					for (int i = 0; i < SmallCount; i++)
					{
						Assert.That(itemsSizes[i], Is.EqualTo(session.Dequeue()?.Length ?? -1));
					}

					session.Flush();
				}
			}
		}

		private const int LargeCount = 1000000;
		private const int SmallCount = 500;

	}
}