namespace ModernDiskQueue.Benchmarks.Helpers
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;

    public static class CsvFileHelper
    {
        public static string csvFilePath = $"c:\\perf\\FileSizePerformance-{DateTime.Now.ToString("yyyyMMdd")}-{DateTime.Now.ToString("HHmmss")}.csv";
        public static string csvResolvedPath;

        static CsvFileHelper()
        {
            csvResolvedPath = Path.GetFullPath(csvFilePath);
        }

        /// <summary>
        /// Appends a line to a CSV file, creating the file if it does not exist.
        /// No headers are written.
        /// </summary>
        /// <param name="csvLine">The CSV-formatted string to append.</param>
        public static void AppendLineToCsv(string csvLine)
        {
            // Ensure the directory exists
            var directory = Path.GetDirectoryName(csvFilePath);
            Console.WriteLine($"// Writing data to {csvFilePath}");
            Console.WriteLine($"// If I used resolved path: {csvResolvedPath}");

            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            // Append the line, creating the file if it doesn't exist
            using (var writer = new StreamWriter(csvFilePath, append: true))
            {
                writer.WriteLine(csvLine);
            }
        }

        /// <summary>
        /// Reads a CSV file (no headers) and parses each row into a tuple of (string, int, long).
        /// </summary>
        /// <returns>List of tuples: (StrategyName, Iteration, FileSize)</returns>
        public static List<(string StrategyName, int Iteration, long FileSize)> ReadCsvToTupleList()
        {
            var result = new List<(string, int, long)>();
            Console.WriteLine($"Resolved path: {csvResolvedPath}");

            string? filePath = Path.GetDirectoryName(csvFilePath);

            foreach (var line in File.ReadLines(filePath))
            {
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }

                var parts = line.Split(',', StringSplitOptions.TrimEntries);
                if (parts.Length != 3)
                {
                    continue; // skip malformed lines
                }

                if (int.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out int iteration) &&
                    long.TryParse(parts[2], NumberStyles.Integer, CultureInfo.InvariantCulture, out long fileSize))
                {
                    result.Add((parts[0], iteration, fileSize));
                }
            }

            return result;
        }

        /// <summary>
        /// Gets the file path of the CSV file.
        /// </summary>
        /// <returns>Absolute path of the csv data file.</returns>
        public static string GetFilePath()
        {
            string? filePath = Path.GetFullPath(csvFilePath);
            return filePath ?? string.Empty;
        }
    }
}
