using System;
using System.IO;

namespace ModernDiskQueue.Tests.Helpers
{
    internal static class FileTools
    {
        internal static void AttemptManualCleanup(string path)
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
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in manual cleanup: {ex.Message}");
            }
        }

    }
}
