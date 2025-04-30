namespace TestTrimmedExecutable
{
    using Microsoft.Extensions.DependencyInjection;
    using ModernDiskQueue;
    using ModernDiskQueue.DependencyInjection;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;

    public static class Program
    {
        private const string folderNameSimpleQueue = "simpleQueue";
        private const string folderNameComplexQueue = "complexQueue";
        private static bool isConsoleLoggingEnabled = false; // note this will pollute output for testing.

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
                    throw new ArgumentException("Expecting two arguments in the form of test={1 | 2} value={[int] | [DateTimeOffset string]");
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
                    isConsoleLoggingEnabled = writeoutput.Equals("true", StringComparison.InvariantCultureIgnoreCase);
                    if (isConsoleLoggingEnabled) Console.WriteLine($"Console logging enabled = {isConsoleLoggingEnabled}");
                }

                if (arguments.TryGetValue("test", out string? testToRun))
                {
                    if (arguments.TryGetValue("value", out string? inputArgument))
                    {
                        if (!string.IsNullOrEmpty(testToRun) && !string.IsNullOrEmpty(inputArgument))
                        {
                            int inputInt = 0;
                            DateTimeOffset inputDate;
                            var factory = serviceProvider.GetRequiredService<PersistentQueueFactory>();

                            switch (arguments["test"])
                            {
                                case "1":
                                    if (int.TryParse(inputArgument, out inputInt))
                                    {
                                        Console.WriteLine(TestSimpleObjectQueueing(inputInt));
                                    }
                                    else
                                    {
                                        throw new ArgumentOutOfRangeException(nameof(args), "Input integer for simple object test could not be parsed.");
                                    }
                                    break;
                                case "2":
                                    if (DateTimeOffset.TryParse(inputArgument, out inputDate))
                                    {
                                        Console.WriteLine(TestComplexObjectQueueing(inputDate));
                                    }
                                    else
                                    {
                                        throw new ArgumentOutOfRangeException(nameof(args), "Input DateTimeOffset for complex object test could not be parsed.");
                                    }
                                    break;
                                case "3":
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
        private static void ConfigureServices(IServiceCollection services)
        {
            // Register PersistentQueueFactory as a singleton
            services.AddModernDiskQueue();
        }

        public static int TestSimpleObjectQueueing(int inputInt)
        {
            int outputInt;
            if (isConsoleLoggingEnabled) Console.WriteLine("Creating queue for simple object");
            PersistentQueue<int> queue = new(folderNameSimpleQueue);
            try
            {
                if (isConsoleLoggingEnabled) Console.WriteLine($"Existing items: {queue.EstimatedCountOfItemsInQueue}");
                if (isConsoleLoggingEnabled) Console.WriteLine("Enqueueing object");
                using (var session = queue.OpenSession())
                {
                    session.Enqueue(inputInt);
                    session.Flush();
                }
                if (isConsoleLoggingEnabled) Console.WriteLine("Dequeueing object");
                using (var session = queue.OpenSession())
                {
                    outputInt = session.Dequeue();
                    session.Flush();
                }
            }
            finally
            {
                if (isConsoleLoggingEnabled) Console.WriteLine("Cleaning up");
                if (isConsoleLoggingEnabled) Console.WriteLine($"Residual items: {queue.EstimatedCountOfItemsInQueue}");
                queue.HardDelete(false);
                DeleteFolderAndFiles(folderNameSimpleQueue);
            }
            return outputInt;
        }

        public static async Task<int> TestSimpleObjectQueueingAsync(int inputInt, PersistentQueueFactory factory)
        {
            int outputInt;
            if (isConsoleLoggingEnabled) Console.WriteLine("Creating queue for simple object");
            await using (PersistentQueue<int> queue = await factory.CreateAsync<int>(folderNameSimpleQueue))
            {
                try
                {
                    if (isConsoleLoggingEnabled) Console.WriteLine($"Existing items: {await queue.GetEstimatedCountOfItemsInQueueAsync()}");
                    if (isConsoleLoggingEnabled) Console.WriteLine("Opening Session..");
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        if (isConsoleLoggingEnabled) Console.WriteLine("Enqueueing object..");
                        await session.EnqueueAsync(inputInt);
                        if (isConsoleLoggingEnabled) Console.WriteLine("Flushing session..");
                        await session.FlushAsync();
                    }
                    if (isConsoleLoggingEnabled) Console.WriteLine("Dequeueing object");
                    await using (var session = await queue.OpenSessionAsync())
                    {
                        outputInt = await session.DequeueAsync();
                        await session.FlushAsync();
                    }
                }
                catch (Exception ex)
                {
                    if (isConsoleLoggingEnabled) Console.WriteLine($"Error occurred during queueing or dequeueing operation. {ex.Message}");
                    throw;
                }
                finally
                {
                    if (isConsoleLoggingEnabled) Console.WriteLine("Cleaning up");
                    if (isConsoleLoggingEnabled) Console.WriteLine($"Residual items: {await queue.GetEstimatedCountOfItemsInQueueAsync()}");
                    await queue.HardDeleteAsync(false);
                    DeleteFolderAndFiles(folderNameSimpleQueue);
                }
            }
            return outputInt;
        }

        public static DateTimeOffset TestComplexObjectQueueing(DateTimeOffset submittedTime)
        {
            DateTimeOffset retrievedTime;
            if (isConsoleLoggingEnabled) Console.WriteLine("Creating queue");
            PersistentQueue<Report> queue = new(folderNameComplexQueue);
            try
            {
                Report myTestReport = new()
                {
                    Id = new(),
                    ReportType = 2,
                    DataField = "test",
                    LocalTime = submittedTime,
                };
                if (isConsoleLoggingEnabled) Console.WriteLine($"Existing items: {queue.EstimatedCountOfItemsInQueue}");
                if (isConsoleLoggingEnabled) Console.WriteLine("Enqueueing object");
                using (var session = queue.OpenSession())
                {
                    // This will serialize our test object.
                    session.Enqueue(myTestReport);
                    session.Flush();
                }
                if (isConsoleLoggingEnabled) Console.WriteLine("Dequeueing object");
                using (var session = queue.OpenSession())
                {
                    Report? data = session.Dequeue();
                    session.Flush();
                    retrievedTime = (data != null) ? data.LocalTime : DateTimeOffset.Now;
                }
            }
            finally
            {
                if (isConsoleLoggingEnabled) Console.WriteLine("Cleaning up...");
                if (isConsoleLoggingEnabled) Console.WriteLine($"Residual items: {queue.EstimatedCountOfItemsInQueue}");
                queue.HardDelete(false);
                DeleteFolderAndFiles(folderNameComplexQueue);
            }
            return retrievedTime;

        }

        public static async Task<DateTimeOffset> TestComplexObjectQueueingAsync(DateTimeOffset submittedTime, PersistentQueueFactory factory)
        {
            DateTimeOffset retrievedTime;
            if (isConsoleLoggingEnabled) Console.WriteLine("Creating queue");
            PersistentQueue<Report> queue = await factory.CreateAsync<Report>(folderNameComplexQueue);
            try
            {
                Report myTestReport = new()
                {
                    Id = new(),
                    ReportType = 2,
                    DataField = "test",
                    LocalTime = submittedTime,
                };
                if (isConsoleLoggingEnabled) Console.WriteLine($"Existing items: {await queue.GetEstimatedCountOfItemsInQueueAsync()}");
                if (isConsoleLoggingEnabled) Console.WriteLine("Enqueueing object");
                using (var session = await queue.OpenSessionAsync())
                {
                    // This will serialize our test object.
                    await session.EnqueueAsync(myTestReport);
                    await session.FlushAsync();
                }
                if (isConsoleLoggingEnabled) Console.WriteLine("Dequeueing object");
                using (var session = await queue.OpenSessionAsync())
                {
                    Report? data = await session.DequeueAsync();
                    await session.FlushAsync();
                    retrievedTime = (data != null) ? data.LocalTime : DateTimeOffset.Now;
                }
            }
            finally
            {
                if (isConsoleLoggingEnabled) Console.WriteLine("Cleaning up...");
                if (isConsoleLoggingEnabled) Console.WriteLine($"Residual items: {queue.EstimatedCountOfItemsInQueue}");
                await queue.HardDeleteAsync(false);
                DeleteFolderAndFiles(folderNameComplexQueue);
            }
            return retrievedTime;

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