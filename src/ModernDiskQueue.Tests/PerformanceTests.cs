namespace ModernDiskQueue.Tests
{
    using ModernDiskQueue.Tests.Models;
    using NUnit.Framework;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading;

    [TestFixture, Explicit, SingleThreaded]
    public class PerformanceTests : PersistentQueueTestsBase
    {
        private const int LargeCount = 1000000;
        private const int SmallCount = 500;

        protected override string QueuePath => "PerformanceTests";

        [Test, Description(
            "With a mid-range SSD, this is some 20x slower " +
            "than with a single flush (depends on disk speed)")]
        public void Enqueue_million_items_with_100_flushes()
        {
            using (var queue = new PersistentQueue(QueuePath))
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
            using (var queue = new PersistentQueue(QueuePath))
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
            using (var queue = new PersistentQueue(QueuePath)) { queue.HardDelete(false); }

            var rnd = new Random();
            var threads = new Thread[200];

            // enqueue threads
            for (int i = 0; i < 100; i++)
            {
                var j = i;
                threads[i] = new Thread(() =>
                {
                    for (int k = 0; k < 10; k++)
                    {
                        Thread.Sleep(rnd.Next(5));
                        using (var q = PersistentQueue.WaitFor(QueuePath, TimeSpan.FromSeconds(50)))
                        {
                            using var s = q.OpenSession();
                            s.Enqueue(Encoding.ASCII.GetBytes($"Thread {j} enqueue {k}"));
                            s.Flush();
                        }
                    }
                })
                { IsBackground = true };
                threads[i].Start();
            }

            // dequeue single
            Thread.Sleep(1000);
            var count = 0;
            while (true)
            {
                byte[]? bytes;
                using (var q = PersistentQueue.WaitFor(QueuePath, TimeSpan.FromSeconds(50)))
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
		/// Just as with the Enqueue_million_items_with_100_flushes test, this is heavily dependent on the host
		/// system performance. To be able to complete in time, one may need to adjust the sleep duration before
		/// the dequeue operation starts in order to give the enqueue operation more of a head start. One thread
		/// performing 1000 write operations doesn't complete as quickly as 100 threads doing read operations.
		/// To some extent the read_heavy and write_heavy tests can be used as performance profilers if you
		/// know the demand that will be placed on the queue and can run the tests on a system with a similar
		/// performance profile as your deployment target.
        /// </summary>
        [Test]
        public void read_heavy_multi_thread_workload()
        {
            DateTime testStartTime = DateTime.Now;
            using (var queue = new PersistentQueue(QueuePath))
            {
                queue.HardDelete(false);
            }

            // shared counter for total dequeues
            int totalDequeues = 0;

            // enqueue 1000 items in a single thread.
            var enqueueThread = new Thread(() =>
            {
                var enqueueStartTime = DateTime.Now;
                for (int i = 0; i < 1000; i++)
                {
                    using var q = PersistentQueue.WaitFor(QueuePath, TimeSpan.FromSeconds(50));
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
                            using (var q = PersistentQueue.WaitFor(QueuePath, TimeSpan.FromSeconds(80)))
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
        public void PerformanceProfiler_ReadHeavyMultiThread_StatsCollection()
        {
            // Pre-allocate metrics collections to avoid resizing
            var metrics = new ConcurrentQueue<OperationMetrics>();

            // Arrange test parameters
            int numberOfDequeueThreads = 100;
            int enqueueHeadstartInSeconds = 18;
            int timeoutForQueueCreationDuringDequeueInSeconds = 100;
            int timeoutForQueueCreationDuringEnqueueInSeconds = 50;
            int timeoutForDequeueThreadsInMinutes = 3;
            int timeoutForEnqueueThreadInMinutes = 3;
            var totalDequeues = 0;
            var successfulThreads = 0;
            var failedThreads = new ConcurrentBag<(int threadId, string reason)>();

            DateTime testStartTime = DateTime.Now;
            using (var queue = new PersistentQueue(QueuePath))
            {
                queue.HardDelete(false);
            }

            var enqueueCompleted = new ManualResetEventSlim(false);
            var dequeueCompletedEvents = new ManualResetEvent[numberOfDequeueThreads];

            for (int i = 0; i < dequeueCompletedEvents.Length; i++)
            {
                dequeueCompletedEvents[i] = new ManualResetEvent(false);
            }

            // enqueue thread
            var enqueueThread = new Thread(() =>
            {
                var threadId = Environment.CurrentManagedThreadId;
                var enqueueStartTime = DateTime.Now;
                var stopwatch = new Stopwatch();

                try
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        var metric = new OperationMetrics
                        {
                            ThreadId = threadId,
                            ItemNumber = i,
                            Operation = "enqueue",
                            Time = DateTime.Now
                        };

                        stopwatch.Restart();
                        using var q = PersistentQueue.WaitFor(QueuePath, TimeSpan.FromSeconds(timeoutForQueueCreationDuringEnqueueInSeconds));
                        {
                            metric.QueueCreateTime = stopwatch.Elapsed;

                            stopwatch.Restart();
                            using (var s = q.OpenSession())
                            {
                                metric.SessionCreateTime = stopwatch.Elapsed;

                                stopwatch.Restart();
                                s.Enqueue(Encoding.ASCII.GetBytes($"Enqueued item {i}"));
                                metric.OperationTime = stopwatch.Elapsed;

                                stopwatch.Restart();
                                s.Flush();
                                metric.FlushTime = stopwatch.Elapsed;
                            }
                        }
                        metrics.Enqueue(metric);
                    }
                    Console.WriteLine($"Enqueue thread finished, took {(DateTime.Now - enqueueStartTime).TotalSeconds:F2} seconds.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Enqueue thread failed: {ex.Message}");
                }
                finally
                {
                    enqueueCompleted.Set();
                }
            })
            {
                IsBackground = true
            };

            enqueueThread.Start();

            // Wait for the enqueue thread to signal completion or for 18 seconds to pass
            if (!enqueueCompleted.Wait(TimeSpan.FromSeconds(enqueueHeadstartInSeconds)))
            {
                Console.WriteLine($"Warning: Enqueue thread did not complete within {enqueueHeadstartInSeconds} second headstart.");
            }

            var rnd = new Random();
            var threads = new Thread[numberOfDequeueThreads];
            DateTime dequeueStartTime = DateTime.Now;

            try
            {
                // dequeue threads
                for (int i = 0; i < threads.Length; i++)
                {
                    var completionEvent = dequeueCompletedEvents[i];
                    threads[i] = new Thread(() =>
                    {
                        var threadIndex = Environment.CurrentManagedThreadId;
                        var stopwatch = new Stopwatch();
                        var count = 10;

                        try
                        {
                            while (count > 0)
                            {
                                Thread.Sleep(rnd.Next(5));

                                var metric = new OperationMetrics
                                {
                                    ThreadId = threadIndex,
                                    Operation = "dequeue",
                                    Time = DateTime.Now
                                };

                                stopwatch.Restart();
                                using var q = PersistentQueue.WaitFor(QueuePath, TimeSpan.FromSeconds(timeoutForQueueCreationDuringDequeueInSeconds));
                                {

                                    metric.QueueCreateTime = stopwatch.Elapsed;

                                    stopwatch.Restart();
                                    using var s = q.OpenSession();
                                    {
                                        metric.SessionCreateTime = stopwatch.Elapsed;

                                        stopwatch.Restart();
                                        var data = s.Dequeue();
                                        metric.OperationTime = stopwatch.Elapsed;

                                        if (data != null)
                                        {
                                            count--;
                                            metric.ItemNumber = Interlocked.Increment(ref totalDequeues);

                                            stopwatch.Restart();
                                            s.Flush();
                                            metric.FlushTime = stopwatch.Elapsed;

                                            metrics.Enqueue(metric);
                                        }
                                    }
                                }
                            }
                            Interlocked.Increment(ref successfulThreads);
                        }
                        catch (TimeoutException ex)
                        {
                            Console.WriteLine($"Thread {threadIndex} timed out trying to create the queue: {ex}");
                            failedThreads.Add((threadIndex, ex.Message));
                        }
                        catch (Exception ex)
                        {
                            failedThreads.Add((threadIndex, ex.Message));
                        }
                        finally
                        {
                            completionEvent.Set();
                        }
                    })
                    { IsBackground = true };
                    threads[i].Start();
                }

                for (int e = 0; e < threads.Length; e++)
                {
                    if (!dequeueCompletedEvents[e].WaitOne(TimeSpan.FromMinutes(timeoutForDequeueThreadsInMinutes)))
                    {
                        failedThreads.Add((e, "timeout"));
                    }
                }

                if (!enqueueCompleted.Wait(TimeSpan.FromMinutes(timeoutForEnqueueThreadInMinutes)))
                {
                    Console.WriteLine("Enqueue thread timed out.");
                }
            }
            finally
            {
                // Clean up resources
                for (int i = 0; i < dequeueCompletedEvents.Length; i++)
                {
                    dequeueCompletedEvents[i].Dispose();
                }
                enqueueCompleted.Dispose();

                GeneratePerformanceReport(
                    metrics.ToList(),
                    testStartTime,
                    dequeueStartTime,
                    totalDequeues,
                    successfulThreads,
                    failedThreads);

                Assert.That(successfulThreads, Is.EqualTo(threads.Length), "Not all threads completed successfully.");
            }
        }

        [Test]
        public void Enqueue_and_dequeue_million_items_same_queue()
        {
            using (var queue = new PersistentQueue(QueuePath))
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
            using (var queue = new PersistentQueue(QueuePath))
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

            using (var queue = new PersistentQueue(QueuePath))
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
            using (var queue = new PersistentQueue(QueuePath))
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

            using (var queue = new PersistentQueue(QueuePath))
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

        private static void GeneratePerformanceReport(
            List<OperationMetrics> metrics,
            DateTime testStartTime,
            DateTime dequeueStartTime,
            int totalDequeues,
            int successfulThreads,
            IEnumerable<(int threadId, string reason)> failedThreads)
        {
            var totalTime = (DateTime.Now - testStartTime).TotalSeconds;
            var dequeueTime = (DateTime.Now - dequeueStartTime).TotalSeconds;

            var enqueueMetrics = metrics.Where(m => m.Operation == "enqueue").ToList();
            var dequeueMetrics = metrics.Where(m => m.Operation == "dequeue").ToList();

            Console.WriteLine("\n=== Overall Performance Report ===");
            Console.WriteLine($"Total test time: {totalTime:F2} seconds");
            Console.WriteLine($"Total successful dequeues: {totalDequeues}");
            Console.WriteLine($"Successful threads: {successfulThreads}/100");

            if (failedThreads.Any())
            {
                Console.WriteLine("\nFailed Threads:");
                foreach (var (threadId, reason) in failedThreads)
                {
                    Console.WriteLine($"Thread {threadId}: {reason}");
                }
            }

            if (enqueueMetrics.Count != 0)
            {
                Console.WriteLine("\n=== Enqueue Performance ===");
                PrintOperationStats("Enqueue", enqueueMetrics);
            }

            if (dequeueMetrics.Count != 0)
            {
                Console.WriteLine("\n=== Dequeue Performance ===");
                PrintOperationStats("Dequeue", dequeueMetrics);
            }

            // Overall throughput analysis
            Console.WriteLine("\n=== Throughput Analysis ===");
            var enqueueRate = enqueueMetrics.Count / (dequeueStartTime - testStartTime).TotalSeconds;
            var dequeueRate = dequeueMetrics.Count / dequeueTime;
            Console.WriteLine($"Enqueue rate: {enqueueRate:F2} items/second");
            Console.WriteLine($"Dequeue rate: {dequeueRate:F2} items/second");
        }

        private static void PrintOperationStats(string operation, List<OperationMetrics> metrics)
        {
            var totalOps = metrics.Count;
            var avgTotal = metrics.Average(m =>
                (m.QueueCreateTime + m.SessionCreateTime + m.OperationTime + m.FlushTime).TotalMilliseconds);

            Console.WriteLine($"Total {operation}s: {totalOps}");
            Console.WriteLine($"Average time per operation: {avgTotal:F2}ms");

            Console.WriteLine("\nOperation Breakdown (averages in ms):");
            Console.WriteLine($"Queue Creation: {metrics.Average(m => m.QueueCreateTime.TotalMilliseconds):F2}");
            Console.WriteLine($"Session Creation: {metrics.Average(m => m.SessionCreateTime.TotalMilliseconds):F2}");
            Console.WriteLine($"{operation} Operation: {metrics.Average(m => m.OperationTime.TotalMilliseconds):F2}");
            Console.WriteLine($"Flush Operation: {metrics.Average(m => m.FlushTime.TotalMilliseconds):F2}");

            var queueLockTimes = metrics.Select(m =>
                (m.QueueCreateTime).TotalMilliseconds)
                .OrderBy(t => t)
                .ToList();


            var totalTimes = metrics.Select(m =>
                (m.QueueCreateTime + m.SessionCreateTime + m.OperationTime + m.FlushTime).TotalMilliseconds)
                .OrderBy(t => t)
                .ToList();

            Console.WriteLine("\nEnqueue Operation Total Latency Percentiles (ms):");
            Console.WriteLine($"P50: {GetPercentile(totalTimes, 0.5):F2}");
            Console.WriteLine($"P90: {GetPercentile(totalTimes, 0.9):F2}");
            Console.WriteLine($"P95: {GetPercentile(totalTimes, 0.95):F2}");
            Console.WriteLine($"P99: {GetPercentile(totalTimes, 0.99):F2}");



            // Show 5 slowest operations
            var slowest = metrics.OrderByDescending(m =>
                (m.QueueCreateTime + m.SessionCreateTime + m.OperationTime + m.FlushTime).TotalMilliseconds)
                .Take(5);

            Console.WriteLine($"\nSlowest {operation} Operations:");
            foreach (var op in slowest)
            {
                Console.WriteLine(
                    $"Thread {op.ThreadId}, Item {op.ItemNumber}: " +
                    $"Total {(op.QueueCreateTime + op.SessionCreateTime + op.OperationTime + op.FlushTime).TotalMilliseconds:F2}ms " +
                    $"(Queue: {op.QueueCreateTime.TotalMilliseconds:F2}ms, " +
                    $"Session: {op.SessionCreateTime.TotalMilliseconds:F2}ms, " +
                    $"Op: {op.OperationTime.TotalMilliseconds:F2}ms, " +
                    $"Flush: {op.FlushTime.TotalMilliseconds:F2}ms)");
            }

            Console.WriteLine($"\n{operation} Lock Time Latency Percentiles (ms):");
            Console.WriteLine($"P50: {GetPercentile(queueLockTimes, 0.5):F2}");
            Console.WriteLine($"P90: {GetPercentile(queueLockTimes, 0.9):F2}");
            Console.WriteLine($"P95: {GetPercentile(queueLockTimes, 0.95):F2}");
            Console.WriteLine($"P99: {GetPercentile(queueLockTimes, 0.99):F2}");
        }

        private static double GetPercentile(List<double> sequence, double percentile)
        {
            var sorted = sequence.OrderBy(x => x).ToList();
            int N = sorted.Count;
            double n = (N - 1) * percentile + 1;
            if (n == 1) return sorted[0];
            if (n == N) return sorted[N - 1];
            int k = (int)n;
            double d = n - k;
            return sorted[k - 1] + d * (sorted[k] - sorted[k - 1]);
        }
    }
}