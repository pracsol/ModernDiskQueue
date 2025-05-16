// <copyright file="PersistentQueueTestsBase.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>

// ReSharper disable PossibleNullReferenceException
// ReSharper disable AssignNullToNotNullAttribute
namespace ModernDiskQueue.Tests
{
    using System;
    using System.IO;
    using NUnit.Framework;

    public abstract class PersistentQueueTestsBase
    {
        private static readonly object _lock = new object();

        protected abstract string QueuePath { get; }

        [SetUp]
        public void Setup()
        {
            RebuildPath();
        }

        /// <summary>
        /// This ensures that we release all files before we complete a test.
        /// </summary>
        [TearDown]
        public void Teardown()
        {
            RebuildPath();
        }

        private void RebuildPath()
        {
            lock (_lock)
            {
                try
                {
                    if (Directory.Exists(QueuePath))
                    {
                        var files = Directory.GetFiles(QueuePath, "*", SearchOption.AllDirectories);
                        Array.Sort(files, (s1, s2) => s2.Length.CompareTo(s1.Length)); // sort by length descending
                        foreach (var file in files)
                        {
                            File.Delete(file);
                        }

                        Directory.Delete(QueuePath, true);
                    }
                }
                catch (UnauthorizedAccessException)
                {
                    Console.WriteLine("Not allowed to delete queue directory. May fail later");
                }
                catch (IOException)
                {
                    // Covers "The process cannot access the file because it is being used by another process"
                    Console.WriteLine("Not allowed to delete queue directory. May fail later");
                }
            }
        }
    }
}