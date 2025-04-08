namespace TestTrimmedExecutable
{
    using ModernDiskQueue;
    using System.IO;
    using System.Runtime.Serialization;

    public static class Program
    {
        private const string folderNameSimpleQueue = "simpleQueue";
        private const string folderNameComplexQueue = "complexQueue";
        private static readonly bool isConsoleLoggingEnabled = false; // note this will pollute output for testing.

        public static void Main(string[] args)
        {
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
                if (arguments.TryGetValue("test", out string? testToRun))
                {
                    if (arguments.TryGetValue("value", out string? inputArgument))
                    {
                        if (!string.IsNullOrEmpty(testToRun) && !string.IsNullOrEmpty(inputArgument))
                        {
                            switch (arguments["test"])
                            {
                                case "1":
                                    if (int.TryParse(inputArgument, out int inputInt))
                                    {
                                        Console.WriteLine(TestSimpleObjectQueueing(inputInt));
                                    }
                                    else
                                    {
                                        throw new ArgumentOutOfRangeException(nameof(args), "Input integer for simple object test could not be parsed.");
                                    }
                                    break;
                                case "2":
                                    if (DateTimeOffset.TryParse(inputArgument, out DateTimeOffset inputDate))
                                    {
                                        Console.WriteLine(TestComplexObjectQueueing(inputDate));
                                    }
                                    else
                                    {
                                        throw new ArgumentOutOfRangeException(nameof(args), "Input DateTimeOffset for complex object test could not be parsed.");
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

    [Serializable]
    internal class Report
    {
        public Guid Id { get; set; } = new();
        public int ReportType { get; set; }
        public string DataField { get; set; } = string.Empty;
        public DateTimeOffset LocalTime { get; set; }
    }
}