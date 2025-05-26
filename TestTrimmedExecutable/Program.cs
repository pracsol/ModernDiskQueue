// -----------------------------------------------------------------------
// <copyright file="Program.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace TestTrimmedExecutable
{
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using Microsoft.Extensions.DependencyInjection;
    using ModernDiskQueue;
    using ModernDiskQueue.DependencyInjection;

    public static class Program
    {
        private const string FolderNameSimpleQueue = "simpleQueue";
        private const string FolderNameComplexQueue = "complexQueue";
        private const string FolderNameSimpleQueueAsync = "simpleQueueAsync";
        private const string FolderNameComplexQueueAsync = "complexQueueAsync";
        private static bool _isConsoleLoggingEnabled = false; // note this will pollute output for testing, so only enable if you're running this project directly for debug purposes.

        public static async Task Main(string[] args)
        {
            // Set up the DI container
            var serviceCollection = new ServiceCollection();
            ConfigureServices(serviceCollection);
            var serviceProvider = serviceCollection.BuildServiceProvider();

            int exitCode = 0;
            try
            {
                var arguments = new Dictionary<string, string>();
                if (args.Length < 2)
                {
                    throw new ArgumentException("Expecting two arguments in the form of test={1 | 2 | 3 | 4} value={[int] | [DateTimeOffset string]");
                }

                foreach (string argument in args)
                {
                    string[] splitted = argument.Split('=');

                    if (splitted.Length == 2)
                    {
                        arguments[splitted[0]] = splitted[1];
                    }
                }

                if (arguments.TryGetValue("writeoutput", out string? writeoutput))
                {
                    _isConsoleLoggingEnabled = writeoutput.Equals("true", StringComparison.InvariantCultureIgnoreCase);
                    if (_isConsoleLoggingEnabled)
                    {
                        Console.WriteLine($"Console logging enabled = {_isConsoleLoggingEnabled}");
                    }
                }

                if (arguments.TryGetValue("test", out string? testToRun))
                {
                    if (arguments.TryGetValue("value", out string? inputArgument))
                    {
                        if (!string.IsNullOrEmpty(testToRun) && !string.IsNullOrEmpty(inputArgument))
                        {
                            int inputInt = 0;
                            DateTimeOffset inputDate;
                            var factory = serviceProvider.GetRequiredService<IPersistentQueueFactory>();

                            switch (arguments["test"])
                            {
                                case "1":
                                    // Case 1 tests the (de)serialization of a simple object (int) using the default serializer with the sync API.
                                    if (int.TryParse(inputArgument, out inputInt))
                                    {
                                        Console.WriteLine(TestSimpleObjectQueueing(inputInt));
                                    }
                                    else
                                    {
                                        throw new ArgumentOutOfRangeException(nameof(args), "Input integer for simple object sync test could not be parsed.");
                                    }

                                    break;
                                case "2":
                                    // Case 2 tests the (de)serialization of a complex object (DateTimeOffset) using the default serializer with the sync API.
                                    if (DateTimeOffset.TryParse(inputArgument, out inputDate))
                                    {
                                        Console.WriteLine(TestComplexObjectQueueing(inputDate));
                                    }
                                    else
                                    {
                                        throw new ArgumentOutOfRangeException(nameof(args), "Input DateTimeOffset for complex object sync test could not be parsed.");
                                    }

                                    break;
                                case "3":
                                    // Case 3 tests the (de)serialization of a simple object (int) using the default serializer with the async API.
                                    if (int.TryParse(inputArgument, out inputInt))
                                    {
                                        var result = await TestSimpleObjectQueueingAsync(inputInt, factory).ConfigureAwait(false);
                                        Console.WriteLine(result);
                                    }
                                    else
                                    {
                                        throw new ArgumentOutOfRangeException(nameof(args), "Input integer for simple object async test could not be parsed.");
                                    }

                                    break;
                                case "4":
                                    // Case 4 tests the (de)serialization of a complex object (DateTimeOffset) using the default serializer with the async API.
                                    if (DateTimeOffset.TryParse(inputArgument, out inputDate))
                                    {
                                        var result = await TestComplexObjectQueueingAsync(inputDate, factory).ConfigureAwait(false);
                                        Console.WriteLine(result);
                                    }
                                    else
                                    {
                                        throw new ArgumentOutOfRangeException(nameof(args), "Input DateTimeOffset for complex object async test could not be parsed.");
                                    }

                                    break;
                            }
                        }
                        else
                        {
                            throw new ArgumentOutOfRangeException(nameof(args), "Argument values not specified correctly. Should be in format of test=[1,2] value=[int, DateTimeOffset]");
                        }
                    }
                    else
                    {
                        throw new ArgumentNullException(nameof(args), "Missing argument 'value'. Specify an integer for the simple object test, or a DateTimeOffset for the complex object test");
                    }
                }
                else
                {
                    throw new ArgumentNullException(nameof(args), "Missing argument 'test'. Supply either 1 for simple object test or 2 for complex object test.");
                }
            }
            catch (InvalidDataContractException ex)
            {
                Console.Error.WriteLine($"Error: {ex.GetType().Name} {ex.Message}");
                exitCode = 2;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Error: {ex.GetType().Name} {ex.Message}");
                exitCode = 1;
            }

            Environment.Exit(exitCode);
        }

        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(PersistentQueueFactory))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(IPersistentQueueFactory))]

        public static int TestSimpleObjectQueueing(int inputInt)
        {
            int outputInt;
            if (_isConsoleLoggingEnabled)
            {
                Console.WriteLine("Creating queue for simple object queueing with sync api");
            }

            PersistentQueue<int> queue = new(FolderNameSimpleQueue);
            try
            {
                if (_isConsoleLoggingEnabled)
                {
                    Console.WriteLine($"Existing items: {queue.EstimatedCountOfItemsInQueue}");
                }

                if (_isConsoleLoggingEnabled)
                {
                    Console.WriteLine("Enqueueing object");
                }

                using (var session = queue.OpenSession())
                {
                    session.Enqueue(inputInt);
                    session.Flush();
                }

                if (_isConsoleLoggingEnabled)
                {
                    Console.WriteLine("Dequeueing object");
                }

                using (var session = queue.OpenSession())
                {
                    outputInt = session.Dequeue();
                    session.Flush();
                }
            }
            catch (Exception ex)
            {
                if (_isConsoleLoggingEnabled)
                {
                    Console.Error.WriteLine($"{ex.Message}");
                }

                throw;
            }
            finally
            {
                if (_isConsoleLoggingEnabled)
                {
                    Console.WriteLine("Cleaning up");
                }

                if (_isConsoleLoggingEnabled)
                {
                    Console.WriteLine($"Residual items: {queue.EstimatedCountOfItemsInQueue}");
                }

                queue.HardDelete(false);
                DeleteFolderAndFiles(FolderNameSimpleQueue);
            }

            return outputInt;
        }

        public static async Task<int> TestSimpleObjectQueueingAsync(int inputInt, IPersistentQueueFactory factory)
        {
            int outputInt;
            if (_isConsoleLoggingEnabled)
            {
                Console.WriteLine("Creating queue for simple object");
            }

            await using (IPersistentQueue<int> queue = await factory.CreateAsync<int>(FolderNameSimpleQueueAsync))
            {
                try
                {
                    if (_isConsoleLoggingEnabled)
                    {
                        Console.WriteLine($"Existing items: {await queue.GetEstimatedCountOfItemsInQueueAsync()}");
                    }

                    if (_isConsoleLoggingEnabled)
                    {
                        Console.WriteLine("Opening Session..");
                    }

                    await using (var session = await queue.OpenSessionAsync())
                    {
                        if (_isConsoleLoggingEnabled)
                        {
                            Console.WriteLine("Enqueueing object..");
                        }

                        await session.EnqueueAsync(inputInt);
                        if (_isConsoleLoggingEnabled)
                        {
                            Console.WriteLine("Flushing session..");
                        }

                        await session.FlushAsync();
                    }

                    if (_isConsoleLoggingEnabled)
                    {
                        Console.WriteLine("Dequeueing object");
                    }

                    await using (var session = await queue.OpenSessionAsync())
                    {
                        outputInt = await session.DequeueAsync();
                        await session.FlushAsync();
                    }
                }
                catch (Exception ex)
                {
                    if (_isConsoleLoggingEnabled)
                    {
                        Console.Error.WriteLine($"{ex.Message}");
                    }

                    throw;
                }
                finally
                {
                    if (_isConsoleLoggingEnabled)
                    {
                        Console.WriteLine("Cleaning up");
                    }

                    if (_isConsoleLoggingEnabled)
                    {
                        Console.WriteLine($"Residual items: {await queue.GetEstimatedCountOfItemsInQueueAsync()}");
                    }

                    await queue.HardDeleteAsync(false);
                    DeleteFolderAndFiles(FolderNameSimpleQueue);
                }
            }

            return outputInt;
        }

        public static DateTimeOffset TestComplexObjectQueueing(DateTimeOffset submittedTime)
        {
            DateTimeOffset retrievedTime;
            if (_isConsoleLoggingEnabled)
            {
                Console.WriteLine("Creating queue for complex object queueing with sync api");
            }

            PersistentQueue<Report> queue = new(FolderNameComplexQueue);
            try
            {
                Report myTestReport = new()
                {
                    Id = Guid.Empty,
                    ReportType = 2,
                    DataField = "test",
                    LocalTime = submittedTime,
                };
                if (_isConsoleLoggingEnabled)
                {
                    Console.WriteLine($"Existing items: {queue.EstimatedCountOfItemsInQueue}");
                }

                if (_isConsoleLoggingEnabled)
                {
                    Console.WriteLine("Enqueueing object");
                }

                using (var session = queue.OpenSession())
                {
                    // This will serialize our test object.
                    session.Enqueue(myTestReport);
                    session.Flush();
                }

                if (_isConsoleLoggingEnabled)
                {
                    Console.WriteLine("Dequeueing object");
                }

                using (var session = queue.OpenSession())
                {
                    Report? data = session.Dequeue();
                    session.Flush();
                    retrievedTime = (data != null) ? data.LocalTime : DateTimeOffset.Now;
                }
            }
            catch (Exception ex)
            {
                if (_isConsoleLoggingEnabled)
                {
                    Console.Error.WriteLine($"{ex.Message}");
                }

                throw;
            }
            finally
            {
                if (_isConsoleLoggingEnabled)
                {
                    Console.WriteLine("Cleaning up...");
                }

                if (_isConsoleLoggingEnabled)
                {
                    Console.WriteLine($"Residual items: {queue.EstimatedCountOfItemsInQueue}");
                }

                queue.HardDelete(false);
                DeleteFolderAndFiles(FolderNameComplexQueue);
            }

            return retrievedTime;
        }

        public static async Task<DateTimeOffset> TestComplexObjectQueueingAsync(DateTimeOffset submittedTime, IPersistentQueueFactory factory)
        {
            DateTimeOffset retrievedTime;
            if (_isConsoleLoggingEnabled)
            {
                Console.WriteLine("Creating queue");
            }

            IPersistentQueue<Report> queue = await factory.CreateAsync<Report>(FolderNameComplexQueueAsync);
            try
            {
                Report myTestReport = new()
                {
                    Id = Guid.Empty,
                    ReportType = 2,
                    DataField = "test",
                    LocalTime = submittedTime,
                };
                if (_isConsoleLoggingEnabled)
                {
                    Console.WriteLine($"Existing items: {await queue.GetEstimatedCountOfItemsInQueueAsync()}");
                }

                if (_isConsoleLoggingEnabled)
                {
                    Console.WriteLine("Enqueueing object");
                }

                await using (var session = await queue.OpenSessionAsync())
                {
                    // This will serialize our test object.
                    await session.EnqueueAsync(myTestReport);
                    await session.FlushAsync();
                }

                if (_isConsoleLoggingEnabled)
                {
                    Console.WriteLine("Dequeueing object");
                }

                await using (var session = await queue.OpenSessionAsync())
                {
                    Report? data = await session.DequeueAsync();
                    await session.FlushAsync();
                    retrievedTime = (data != null) ? data.LocalTime : DateTimeOffset.Now;
                }
            }
            catch (Exception ex)
            {
                if (_isConsoleLoggingEnabled)
                {
                    Console.Error.WriteLine($"{ex.Message}");
                }

                throw;
            }
            finally
            {
                if (_isConsoleLoggingEnabled)
                {
                    Console.WriteLine("Cleaning up...");
                }

                if (_isConsoleLoggingEnabled)
                {
                    Console.WriteLine($"Residual items: {await queue.GetEstimatedCountOfItemsInQueueAsync()}");
                }

                await queue.HardDeleteAsync(false);
                DeleteFolderAndFiles(FolderNameComplexQueue);
                await queue.DisposeAsync();
            }

            return retrievedTime;
        }

        private static void ConfigureServices(IServiceCollection services)
        {
            // services.AddLogging();
            // Register PersistentQueueFactory as a singleton
            services.AddModernDiskQueue();
        }

        private static void DeleteFolderAndFiles(string folderName)
        {
            DirectoryInfo dirInfo = new(folderName);
            if (dirInfo.Exists)
            {
                foreach (var dir in dirInfo.EnumerateDirectories())
                {
                    DeleteFolderAndFiles(dir.FullName);
                }

                dirInfo.Delete(true);
            }
        }
    }
}