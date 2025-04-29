using ModernDiskQueue.Implementation;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
// ReSharper disable AssignNullToNotNullAttribute

// ReSharper disable PossibleNullReferenceException

namespace ModernDiskQueue.Tests
{
    [TestFixture, SingleThreaded]
    public class PersistentQueueTests : PersistentQueueTestsBase
    {
        protected override string QueuePath => "./PersistentQueueTests";

        [Test]
        public void Only_single_instance_of_queue_can_exists_at_any_one_time()
        {
            var invalidOperationException = Assert.Throws<InvalidOperationException>(() =>
            {
                using (new PersistentQueue(QueuePath))
                {
                    // ReSharper disable once ObjectCreationAsStatement
                    new PersistentQueue(QueuePath);
                }
            });

            Assert.That(invalidOperationException.Message, Is.EqualTo("Another instance of the queue is already in action, or directory does not exist"));
        }

        [Test]
        public void If_a_non_running_process_has_a_lock_then_can_start_an_instance()
        {
            Directory.CreateDirectory(QueuePath);
            var lockFilePath = System.IO.Path.Combine(QueuePath, "lock");
            File.WriteAllText(lockFilePath, "78924759045");

            using (new PersistentQueue(QueuePath))
            {
                Assert.Pass();
            }
        }

        [Test]
        public void Can_create_new_queue()
        {
            new PersistentQueue(QueuePath).Dispose();
        }

        [Test]
        public void Corrupt_index_file_should_throw()
        {
            PersistentQueue.DefaultSettings.AllowTruncatedEntries = false;
            var buffer = new List<byte>();
            buffer.AddRange(Guid.NewGuid().ToByteArray());
            buffer.AddRange(Guid.NewGuid().ToByteArray());
            buffer.AddRange(Guid.NewGuid().ToByteArray());

            Directory.CreateDirectory(QueuePath);
            File.WriteAllBytes(System.IO.Path.Combine(QueuePath, "transaction.log"), buffer.ToArray());

            var invalidOperationException = Assert.Throws<UnrecoverableException>(() =>
            {
                // ReSharper disable once ObjectCreationAsStatement
                new PersistentQueue(QueuePath);
            });

            Assert.That(invalidOperationException.Message, Is.EqualTo("Unexpected data in transaction log. Expected to get transaction separator but got unknown data. Tx #1"));
        }

        /// <summary>
        /// This test is not part of the original test suite, but was failing on async side so wanted to understand 
        /// baseline behavior. In the sync operations, the queue folders are successfully deleted, but the dispose method being called
        /// at the end of the HardDelete method which ends up creating a new transaction.log file in the path, meaning the folder gets
        /// recreated. This can be avoided if you set the default setting of <see cref="PersistentQueue.DefaultSettings.TrimTransactionLogOnDispose">= false.
        /// </summary>
        [Test, Explicit]
        public void Can_hard_delete()
        {
            // ARRANGE
            // This flag will be checked to determine if test passes or fails.
            // The intention is that the test fails when HardDeleteAsync fails to completely remove the folder and files.
            // For diagnostic purposes, if this fails I want to try and manually clean files to get direct exceptions thrown
            // for locking issues, that sort of thing. But then I'll check this flag and fail the test if these measures had to be taken.
            bool wasManuallyCleanedUp = false;

            // create the initial queue.
            var queue = new PersistentQueue(QueuePath);
            using (queue.OpenSession()) { }

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
                queue.HardDelete(false);
                if (!Directory.Exists(QueuePath))
                {
                    Assert.Pass("HardDelete cleaned up Queue folder structure.");
                }
                // Check directory state immediately after delete
                Console.WriteLine($"After delete - Directory exists: {Directory.Exists(QueuePath)}");


                // Here I'd normally assert that the directory is deleted, but if it's not I want to try and manually delete
                // for diagnostic purposes. This will set the flag to true and we'll assert on that.

                // Diagnostic: Check if we can manually delete files
                if (Directory.Exists(QueuePath))
                {
                    Console.WriteLine("Attempting manual cleanup:");
                    wasManuallyCleanedUp = true;
                    AttemptManualCleanup(QueuePath);
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
                        AttemptManualCleanup(QueuePath);
                    }

                    Thread.Sleep(retryDelayMs);
                }

                // If the directory still exists after all retries, perform deeper diagnostics
                if (!isDirectoryDeleted)
                {
                    Console.WriteLine("CRITICAL: Directory still exists after max retries!");
                    PerformDeepDiagnostics(QueuePath);
                }

                // ASSERT
                Assert.That(isDirectoryDeleted, Is.True, "Queue directory should have been deleted from HardDelete(false) but is still there.");
                Assert.That(wasManuallyCleanedUp, Is.False, "Queue directory was manually cleaned up, indicating a potential issue with HardDelete not getting lock on files.");
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

        private void AttemptManualCleanup(string path)
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
                Thread.Sleep(500);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in manual cleanup: {ex.Message}");
            }
        }

        private void PerformDeepDiagnostics(string path)
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
                Thread.Sleep(1000);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in deep diagnostics: {ex.Message}");
            }
        }

        [Test]
        public void Dequeueing_from_empty_queue_will_return_null()
        {
            using (var queue = new PersistentQueue(QueuePath))
            using (var session = queue.OpenSession())
            {
                Assert.That(session.Dequeue(), Is.Null);
            }
        }

        [Test]
        public void Can_enqueue_data_in_queue()
        {
            using (var queue = new PersistentQueue(QueuePath))
            using (var session = queue.OpenSession())
            {
                session.Enqueue(new byte[] { 1, 2, 3, 4 });
                session.Flush();
            }
        }

        [Test]
        public void Can_dequeue_data_from_queue()
        {
            using (var queue = new PersistentQueue(QueuePath))
            using (var session = queue.OpenSession())
            {
                session.Enqueue(new byte[] { 1, 2, 3, 4 });
                session.Flush();
                Assert.That(session.Dequeue(), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
            }
        }

        [Test]
        public void Queueing_and_dequeueing_empty_data_is_handled()
        {
            using (var queue = new PersistentQueue(QueuePath))
            using (var session = queue.OpenSession())
            {
                session.Enqueue(new byte[0]);
                session.Flush();
                Assert.That(session.Dequeue(), Is.EqualTo(Array.Empty<byte>()));
            }
        }

        [Test]
        public void Can_enqueue_and_dequeue_data_after_restarting_queue()
        {
            using (var queue = new PersistentQueue(QueuePath))
            using (var session = queue.OpenSession())
            {
                session.Enqueue(new byte[] { 1, 2, 3, 4 });
                session.Flush();
            }

            using (var queue = new PersistentQueue(QueuePath))
            using (var session = queue.OpenSession())
            {
                Assert.That(session.Dequeue(), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                session.Flush();
            }
        }

        [Test]
        public void After_dequeue_from_queue_item_no_longer_on_queue()
        {
            using (var queue = new PersistentQueue(QueuePath))
            using (var session = queue.OpenSession())
            {
                session.Enqueue(new byte[] { 1, 2, 3, 4 });
                session.Flush();
            }

            using (var queue = new PersistentQueue(QueuePath))
            using (var session = queue.OpenSession())
            {
                Assert.That(session.Dequeue(), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                Assert.That(session.Dequeue(), Is.Null);
                session.Flush();
            }
        }

        [Test]
        public void After_dequeue_from_queue_item_no_longer_on_queue_with_queues_restarts()
        {
            using (var queue = new PersistentQueue(QueuePath))
            using (var session = queue.OpenSession())
            {
                session.Enqueue(new byte[] { 1, 2, 3, 4 });
                session.Flush();
            }

            using (var queue = new PersistentQueue(QueuePath))
            using (var session = queue.OpenSession())
            {
                Assert.That(session.Dequeue(), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                session.Flush();
            }

            using (var queue = new PersistentQueue(QueuePath))
            using (var session = queue.OpenSession())
            {
                Assert.That(session.Dequeue(), Is.Null);
                session.Flush();
            }
        }

        [Test]
        public void Not_flushing_the_session_will_revert_dequeued_items()
        {
            using (var queue = new PersistentQueue(QueuePath))
            using (var session = queue.OpenSession())
            {
                session.Enqueue(new byte[] { 1, 2, 3, 4 });
                session.Flush();
            }

            using (var queue = new PersistentQueue(QueuePath))
            using (var session = queue.OpenSession())
            {
                Assert.That(session.Dequeue(), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                //Explicitly omitted: session.Flush();
            }

            using (var queue = new PersistentQueue(QueuePath))
            using (var session = queue.OpenSession())
            {
                Assert.That(session.Dequeue(), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                session.Flush();
            }
        }

        [Test]
        public void Not_flushing_the_session_will_revert_queued_items()
        {
            using (var queue = new PersistentQueue(QueuePath))
            using (var session = queue.OpenSession())
            {
                session.Enqueue(new byte[] { 1, 2, 3, 4 });
            }

            using (var queue = new PersistentQueue(QueuePath))
            using (var session = queue.OpenSession())
            {
                Assert.That(session.Dequeue(), Is.Null);
                session.Flush();
            }
        }

        [Test]
        public void Not_flushing_the_session_will_revert_dequeued_items_two_sessions_same_queue()
        {
            using (var queue = new PersistentQueue(QueuePath))
            using (var session = queue.OpenSession())
            {
                session.Enqueue(new byte[] { 1, 2, 3, 4 });
                session.Flush();
            }

            using (var queue = new PersistentQueue(QueuePath))
            using (var session2 = queue.OpenSession())
            {
                using (var session1 = queue.OpenSession())
                {
                    Assert.That(session1.Dequeue(), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                    //Explicitly omitted: session.Flush();
                }
                Assert.That(session2.Dequeue(), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                session2.Flush();
            }
        }

        [Test]
        public void Two_sessions_off_the_same_queue_cannot_get_same_item()
        {
            using (var queue = new PersistentQueue(QueuePath))
            using (var session = queue.OpenSession())
            {
                session.Enqueue(new byte[] { 1, 2, 3, 4 });
                session.Flush();
            }

            using (var queue = new PersistentQueue(QueuePath))
            using (var session2 = queue.OpenSession())
            using (var session1 = queue.OpenSession())
            {
                Assert.That(session1.Dequeue(), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                Assert.That(session2.Dequeue(), Is.Null);
            }
        }

        [Test]
        public void Items_are_reverted_in_their_original_order()
        {
            using (var queue = new PersistentQueue(QueuePath))
            using (var session = queue.OpenSession())
            {
                session.Enqueue(new byte[] { 1 });
                session.Enqueue(new byte[] { 2 });
                session.Enqueue(new byte[] { 3 });
                session.Enqueue(new byte[] { 4 });
                session.Flush();
            }

            for (int i = 0; i < 4; i++)
            {
                using (var queue = new PersistentQueue(QueuePath))
                using (var session = queue.OpenSession())
                {
                    Assert.That(session.Dequeue(), Is.EqualTo(new byte[] { 1 }), $"Incorrect order on turn {i + 1}");
                    Assert.That(session.Dequeue(), Is.EqualTo(new byte[] { 2 }), $"Incorrect order on turn {i + 1}");
                    Assert.That(session.Dequeue(), Is.EqualTo(new byte[] { 3 }), $"Incorrect order on turn {i + 1}");
                    // Dispose without `session.Flush();`
                }
            }
        }
    }
}