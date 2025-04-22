using ModernDiskQueue.Implementation;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace ModernDiskQueue.Tests
{
    [TestFixture, SingleThreaded]
    public class PersistentQueueTestsAsync : PersistentQueueTestsBase
    {
        protected override string QueuePath => "./PersistentQueueTestsAsync";

        [Test]
        public void Only_single_instance_of_queue_can_exists_at_any_one_time()
        {
            var invalidOperationException = Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await using (await PersistentQueue.CreateAsync(QueuePath))
                {
                    await PersistentQueue.CreateAsync(QueuePath);
                }
            });
            Assert.That(invalidOperationException, Is.Not.Null);
            Assert.That(invalidOperationException.Message, Is.EqualTo("Another instance of the queue is already in action, or directory does not exist"));
        }

        [Test]
        public async Task If_a_non_running_process_has_a_lock_then_can_start_an_instance()
        {
            Directory.CreateDirectory(QueuePath);
            var lockFilePath = System.IO.Path.Combine(QueuePath, "lock");
            File.WriteAllText(lockFilePath, "78924759045");

            await using (await PersistentQueue.CreateAsync(QueuePath))
            {
                Assert.Pass();
            }
        }

        [Test]
        public async Task Can_create_new_queue()
        {
            await using (var queue = await PersistentQueue.WaitForAsync(QueuePath, TimeSpan.FromSeconds(5)))
            {
                Assert.That(queue, Is.Not.Null);
            }
            Console.WriteLine("Completed.");
        }

        [Test]
        public async Task Corrupt_index_file_should_throw()
        {
            PersistentQueue.DefaultSettings.AllowTruncatedEntries = false;
            var buffer = new List<byte>();
            buffer.AddRange(Guid.NewGuid().ToByteArray());
            buffer.AddRange(Guid.NewGuid().ToByteArray());
            buffer.AddRange(Guid.NewGuid().ToByteArray());

            Directory.CreateDirectory(QueuePath);
            File.WriteAllBytes(System.IO.Path.Combine(QueuePath, "transaction.log"), buffer.ToArray());

            var invalidOperationException = Assert.ThrowsAsync<UnrecoverableException>(async () =>
            {
                await using (var q = await PersistentQueue.CreateAsync(QueuePath)) { }
            });

            Assert.That(invalidOperationException, Is.Not.Null);
            Assert.That(invalidOperationException.Message, Is.EqualTo("Unexpected data in transaction log. Expected to get transaction separator but got unknown data. Tx #1"));
        }

        [Test, Explicit]
        public async Task Can_hard_delete()
        {
            // ARRANGE

            // This flag will be checked to determine if test passes or fails.
            // The intention is that the test fails when HardDeleteAsync fails to completely remove the folder and files.
            // For diagnostic purposes, if this fails I want to try and manually clean files to get direct exceptions thrown
            // for locking issues, that sort of thing. But then I'll check this flag and fail the test if these measures had to be taken.
            bool wasManuallyCleanedUp = false;

            // Create initial queue
            await using (var queue = await PersistentQueue.CreateAsync(QueuePath))
            {
                await using (await queue.OpenSessionAsync()) { }
            }

            // Log initial directory state
            Console.WriteLine($"Initial directory state - Exists: {Directory.Exists(QueuePath)}");
            if (Directory.Exists(QueuePath))
            {
                LogDirectoryContents(QueuePath);
            }

            try
            {
                // ACT
                // Call HardDeleteAsync and log when it completes
                Console.WriteLine("Calling HardDeleteAsync(false)");
                await using (var queue = await PersistentQueue.CreateAsync(QueuePath))
                {
                    await queue.HardDeleteAsync(false);
                }
                if (!Directory.Exists(QueuePath))
                {
                    Assert.Pass("HardDeleteAsync cleaned up Queue folder structure.");
                }

                // Force garbage collection to try to release any lingering handles
                // GC.Collect();
                // GC.WaitForPendingFinalizers();
                // GC.Collect(); // Second collection to clean up finalizable objects

                // Check directory state immediately after delete
                Console.WriteLine($"After HardDeleteAsync method - Directory exists: {Directory.Exists(QueuePath)}");

                // Here I'd normally assert that the directory is deleted, but if it's not I want to try and manually delete
                // for diagnostic purposes. This will set the flag to true and we'll assert on that.

                // Diagnostic: Check if we can manually delete files
                if (Directory.Exists(QueuePath))
                {
                    Console.WriteLine("Attempting manual cleanup:");
                    wasManuallyCleanedUp = true;
                    await AttemptManualCleanup(QueuePath);
                }

                // Add retry logic with diagnostics
                const int maxRetries = 10;
                const int retryDelayMs = 1000;
                bool isDirectoryDeleted = false;

                for (int i = 0; i < maxRetries; i++)
                {
                    Console.WriteLine($"Check attempt {i + 1}/{maxRetries} - Directory exists: {Directory.Exists(QueuePath)}");

                    if (!Directory.Exists(QueuePath))
                    {
                        isDirectoryDeleted = true;
                        Console.WriteLine("Directory successfully deleted");
                        break;
                    }

                    // On each retry, log directory contents
                    if (i % 3 == 0 && Directory.Exists(QueuePath))
                    {
                        LogDirectoryContents(QueuePath);

                        // Every third attempt, try manual cleanup again
                        await AttemptManualCleanup(QueuePath);
                    }

                    await Task.Delay(retryDelayMs);
                }

                // If the directory still exists after all retries, perform deeper diagnostics
                if (!isDirectoryDeleted)
                {
                    Console.WriteLine("CRITICAL: Directory still exists after max retries!");
                    await PerformDeepDiagnostics(QueuePath);
                }

                // ASSERT
                Assert.That(isDirectoryDeleted, Is.True, "Queue directory should have been deleted from HardDeleteAsync(false) but is still there.");
                Assert.That(wasManuallyCleanedUp, Is.False, "Queue directory was manually cleaned up, indicating a potential issue with HardDeleteAsync not getting lock on files.");

            }
            catch (SuccessException) { }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception caught: {ex.GetType().Name} - {ex.Message}");
                Console.WriteLine(ex.StackTrace);
                throw;
            }
        }

        // Helper methods for diagnostics
        private void LogDirectoryContents(string path)
        {
            if (!Directory.Exists(path))
            {
                Console.WriteLine($"Directory {path} does not exist");
                return;
            }

            try
            {
                Console.WriteLine($"Contents of directory {path}:");
                var files = Directory.GetFiles(path, "*", SearchOption.AllDirectories);
                Console.WriteLine($"Found {files.Length} files:");

                foreach (var file in files)
                {
                    var info = new FileInfo(file);
                    Console.WriteLine($"- {file} ({info.Length} bytes, {info.LastAccessTime})");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error listing directory: {ex.Message}");
            }
        }

        private async Task AttemptManualCleanup(string path)
        {
            try
            {
                if (!Directory.Exists(path)) return;

                var files = Directory.GetFiles(path, "*", SearchOption.AllDirectories);
                Console.WriteLine($"Attempting to delete {files.Length} files manually");

                foreach (var file in files)
                {
                    try
                    {
                        File.Delete(file);
                        Console.WriteLine($"Successfully deleted {file}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Failed to delete {file}: {ex.Message}");
                    }
                }

                // Try to delete any subdirectories
                var dirs = Directory.GetDirectories(path);
                foreach (var dir in dirs)
                {
                    try
                    {
                        Directory.Delete(dir, true);
                        Console.WriteLine($"Successfully deleted directory {dir}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Failed to delete directory {dir}: {ex.Message}");
                    }
                }

                // Finally try to delete the main directory
                try
                {
                    Directory.Delete(path);
                    Console.WriteLine($"Successfully deleted main directory {path}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Failed to delete main directory: {ex.Message}");
                }

                // Allow time for file system to process deletions
                await Task.Delay(500);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in manual cleanup: {ex.Message}");
            }
        }

        private async Task PerformDeepDiagnostics(string path)
        {
            try
            {
                // Check for any processes that might have locks on the directory
                Console.WriteLine("Performing deep diagnostics...");

                // Try creating a temporary file to test write access
                var tempFilePath = Path.Combine(path, $"test_{Guid.NewGuid()}.tmp");
                try
                {
                    File.WriteAllText(tempFilePath, "test");
                    Console.WriteLine($"Successfully created test file: {tempFilePath}");

                    // Try to delete the test file
                    try
                    {
                        File.Delete(tempFilePath);
                        Console.WriteLine("Successfully deleted test file");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Failed to delete test file: {ex.Message}");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Failed to create test file: {ex.Message}");
                }

                // Check if this is platform-specific
                Console.WriteLine($"Running on: {Environment.OSVersion}");

                // Additional platform-specific diagnostics could be added here

                // Allow time for any pending IO operations
                await Task.Delay(1000);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in deep diagnostics: {ex.Message}");
            }
        }

        [Test]
        public async Task Dequeueing_from_empty_queue_will_return_null()
        {
            await using (var queue = await PersistentQueue.WaitForAsync(QueuePath, TimeSpan.FromSeconds(5)))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    var dequeued = await session.DequeueAsync();
                    Assert.That(dequeued, Is.Null);
                    await session.FlushAsync();
                }
            }
        }

        [Test]
        public async Task Can_enqueue_data_in_queue()
        {
            await using (var queue = await PersistentQueue.WaitForAsync(QueuePath, TimeSpan.FromSeconds(5)))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                    await session.FlushAsync();
                }
            }
            Console.WriteLine("Completed.");
        }

        [Test]
        public async Task Can_dequeue_data_from_queue()
        {
            await using (var queue = await PersistentQueue.WaitForAsync(QueuePath, TimeSpan.FromSeconds(5)))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                    await session.FlushAsync();
                    var dequeued = await session.DequeueAsync();
                    Assert.That(dequeued, Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                }
            }
        }

        [Test]
        public async Task Queueing_and_dequeueing_empty_data_is_handled()
        {
            await using (var queue = await PersistentQueue.WaitForAsync(QueuePath, TimeSpan.FromSeconds(5)))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    await session.EnqueueAsync(Array.Empty<byte>());
                    await session.FlushAsync();
                    var dequeued = await session.DequeueAsync();
                    Assert.That(dequeued, Is.EqualTo(Array.Empty<byte>()));
                }
            }
        }

        [Test]
        public async Task Can_enqueue_and_dequeue_data_after_restarting_queuec()
        {
            // First session: enqueue data
            await using (var queue = await PersistentQueue.WaitForAsync(QueuePath, TimeSpan.FromSeconds(5)))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                    await session.FlushAsync();
                }
            }

            // Second session: dequeue data
            await using (var queue = await PersistentQueue.WaitForAsync(QueuePath, TimeSpan.FromSeconds(5)))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    var dequeued = await session.DequeueAsync();
                    Assert.That(dequeued, Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                    await session.FlushAsync();
                }
            }
        }

        [Test]
        public async Task After_dequeue_from_queue_item_no_longer_on_queue()
        {
            // First session: enqueue data
            await using (var queue = await PersistentQueue.WaitForAsync(QueuePath, TimeSpan.FromSeconds(5)))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                    await session.FlushAsync();
                }
            }

            // Second session: dequeue and verify queue is empty
            await using (var queue = await PersistentQueue.WaitForAsync(QueuePath, TimeSpan.FromSeconds(5)))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    var dequeued = await session.DequeueAsync();
                    Assert.That(dequeued, Is.EqualTo(new byte[] { 1, 2, 3, 4 }));

                    var shouldBeNull = await session.DequeueAsync();
                    Assert.That(shouldBeNull, Is.Null);

                    await session.FlushAsync();
                }
            }
            Console.WriteLine("Completed.");
        }

        [Test]
        public async Task After_dequeue_from_queue_item_no_longer_on_queue_with_queues_restarts()
        {
            await using (var queue = await PersistentQueue.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                    await session.FlushAsync();
                }
            }

            await using (var queue = await PersistentQueue.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    Assert.That(await session.DequeueAsync(), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                    await session.FlushAsync();
                }
            }

            await using (var queue = await PersistentQueue.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    Assert.That(await session.DequeueAsync(), Is.Null);
                    await session.FlushAsync();
                }
            }
            Console.WriteLine("Completed.");
        }

        [Test]
        public async Task Not_flushing_the_session_will_revert_dequeued_items()
        {
            // First session: enqueue data
            {
                await using (var queue = await PersistentQueue.WaitForAsync(QueuePath, TimeSpan.FromSeconds(5)))
                {
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                        await session.FlushAsync();
                    }                    
                }
            }

            // Second session: dequeue but don't flush
            {
                await using (var queue = await PersistentQueue.WaitForAsync(QueuePath, TimeSpan.FromSeconds(5)))
                {
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        var dequeued = await session.DequeueAsync();
                        Assert.That(dequeued, Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                        // Explicitly omit: await session.FlushAsync();
                    }
                }
            }

            // Third session: verify item is still there
            {
                await using (var queue = await PersistentQueue.WaitForAsync(QueuePath, TimeSpan.FromSeconds(5)))
                {
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        var dequeued = await session.DequeueAsync();
                        Assert.That(dequeued, Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                        await session.FlushAsync();
                    }
                }
            }
        }

        [Test]
        public async Task Not_flushing_the_session_will_revert_queued_items()
        {
            // First session: enqueue data but don't flush
            {
                await using (var queue = await PersistentQueue.WaitForAsync(QueuePath, TimeSpan.FromSeconds(5)))
                {
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                    }
                    // Explicitly omit: await session.FlushAsync();
                }
            }

            // Second session: verify queue is empty
            {
                await using (var queue = await PersistentQueue.WaitForAsync(QueuePath, TimeSpan.FromSeconds(5)))
                {
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        var dequeued = await session.DequeueAsync();
                        Assert.That(dequeued, Is.Null);
                        await session.FlushAsync();
                    }
                }
            }
        }

        [Test]
        public async Task Not_flushing_the_session_will_revert_dequeued_items_two_sessions_same_queue()
        {
            await using (var queue = await PersistentQueue.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                    await session.FlushAsync();
                }
            }

            await using (var queue = await PersistentQueue.CreateAsync(QueuePath))
            {
                await using (var session2 = await queue.OpenSessionAsync())
                {
                    await using (var session1 = await queue.OpenSessionAsync())
                    {
                        var dequeued = await session1.DequeueAsync();
                        Assert.That(dequeued, Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                        //Explicitly omitted: session.Flush();
                    }
                    Assert.That(await session2.DequeueAsync(), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                    await session2.FlushAsync();
                }
            }
        }

        [Test]
        public async Task Two_sessions_off_the_same_queue_cannot_get_same_item()
        {
            await using (var queue = await PersistentQueue.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
                    await session.FlushAsync();
                }
            }

            await using (var queue = await PersistentQueue.CreateAsync(QueuePath))
            {
                await using (var session2 = await queue.OpenSessionAsync())
                {
                    await using (var session1 = await queue.OpenSessionAsync())
                    {
                        Assert.That(await session1.DequeueAsync(), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                        Assert.That(await session2.DequeueAsync(), Is.Null);
                    }
                }
            }
        }

        [Test]
        public async Task Items_are_reverted_in_their_original_order()
        {
            await using (var queue = await PersistentQueue.CreateAsync(QueuePath))
            {
                await using (var session = await queue.OpenSessionAsync())
                {
                    await session.EnqueueAsync(new byte[] { 1 });
                    await session.EnqueueAsync(new byte[] { 2 });
                    await session.EnqueueAsync(new byte[] { 3 });
                    await session.EnqueueAsync(new byte[] { 4 });
                    await session.FlushAsync();
                }
            }

            for (int i = 0; i < 4; i++)
            {
                await using (var queue = await PersistentQueue.CreateAsync(QueuePath))
                {
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        Assert.That(await session.DequeueAsync(), Is.EqualTo(new byte[] { 1 }), $"Incorrect order on turn {i + 1}");
                        Assert.That(await session.DequeueAsync(), Is.EqualTo(new byte[] { 2 }), $"Incorrect order on turn {i + 1}");
                        Assert.That(await session.DequeueAsync(), Is.EqualTo(new byte[] { 3 }), $"Incorrect order on turn {i + 1}");
                        // Dispose without `session.Flush();`
                    }
                }
            }
        }

    }
}
