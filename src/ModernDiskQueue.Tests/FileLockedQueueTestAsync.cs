﻿// -----------------------------------------------------------------------
// <copyright file="FileLockedQueueTestAsync.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Tests
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using ModernDiskQueue.Implementation;
    using NUnit.Framework;

    [TestFixture]
    public class FileLockedQueueTestAsync
    {
        private Process? _otherProcess;
        private Process? _currentProcess;
        private int _currentThread;
        private PersistentQueueFactory _factory;

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            _currentProcess = Process.GetCurrentProcess();
            _currentThread = Environment.CurrentManagedThreadId;

            if (File.Exists("TestDummyProcess.exe"))
            {
                _otherProcess = Process.Start("TestDummyProcess.exe");
            }
            else if (File.Exists("TestDummyProcess"))
            {
                _otherProcess = Process.Start("TestDummyProcess");
            }
            else
            {
                Assert.Inconclusive("Can't start test process");
            }

            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.SetMinimumLevel(LogLevel.Information);
                builder.AddConsole();
            });
            _factory = new PersistentQueueFactory(loggerFactory);
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            _otherProcess?.Kill();
        }

        [Test]
        public async Task FileIsLockedAfterPowerFailure_QueueObtainsLock()
        {
            // ARRANGE
            var queueName = GetQueueName();
            WriteLockFile(queueName, _otherProcess!.Id, _currentThread, _otherProcess.StartTime.AddSeconds(1));

            // ACT
            await using var queue = await _factory.CreateAsync<string>(queueName);
        }

        [Test]
        public async Task FileIsLockedByCurrentProcessButWrongThread_ThrowsException()
        {
            // ARRANGE
            var queueName = GetQueueName();
            WriteLockFile(queueName, _currentProcess!.Id, 555, _currentProcess.StartTime);

            try
            {
                // ACT
                using var queue = await _factory.CreateAsync<string>(queueName);
            }
            catch (InvalidOperationException ex)
            {
                // ASSERT
                Assert.That(ex.InnerException?.Message, Is.EqualTo("This queue is locked by another thread in this process. Thread id = 555"));
                Assert.Pass();
            }

            Assert.Fail();
        }

        [Test]
        public async Task FileIsLockedByOtherRunningProcess_ThrowsException()
        {
            // ARRANGE
            var queueName = GetQueueName();
            WriteLockFile(queueName, _otherProcess!.Id, 555, _otherProcess.StartTime);

            try
            {
                // ACT
                using var queue = await _factory.CreateAsync<string>(queueName);
            }
            catch (InvalidOperationException ex)
            {
                // ASSERT
                Assert.That(ex.InnerException?.Message, Is.EqualTo($"This queue is locked by another running process. Process id = {_otherProcess.Id}"));
                Assert.Pass();
            }

            Assert.Fail();
        }

        private static string GetQueueName()
        {
            var valueToTest = "a string";
            var hash = valueToTest.GetHashCode().ToString("X8");
            return $"./LockQueueTests_{hash}";
        }

        private static void WriteLockFile(string queueName, int processId, int threadId, DateTime startTime)
        {
            var lockData = new LockFileData
            {
                ProcessId = processId,
                ThreadId = threadId,
                ProcessStart = ((DateTimeOffset)startTime).ToUnixTimeMilliseconds(),
            };
            Directory.CreateDirectory(queueName);
            File.WriteAllBytes($@"{queueName}\lock", MarshallHelper.Serialize(lockData));
        }
    }
}