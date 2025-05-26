// -----------------------------------------------------------------------
// <copyright file="FileManagement.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Benchmarks.Helpers
{
    internal static class FileManagement
    {
        internal static void AttemptManualCleanup(string path, LogLevelForFileManagement logLevel = LogLevelForFileManagement.All)
        {
            try
            {
                if (!Directory.Exists(path))
                {
                    if (logLevel != LogLevelForFileManagement.None) Console.WriteLine($"Directory {path} does not exist. No cleanup needed.");
                    return;
                }

                var files = Directory.GetFiles(path, "*", SearchOption.AllDirectories);
                if (logLevel != LogLevelForFileManagement.None) Console.WriteLine($"Attempting to delete {files.Length} files manually");

                foreach (var file in files)
                {
                    try
                    {
                        File.Delete(file);
                        if (logLevel == LogLevelForFileManagement.All) Console.WriteLine($"Successfully deleted {file}");
                    }
                    catch (Exception ex)
                    {
                        if (logLevel != LogLevelForFileManagement.None) Console.WriteLine($"Failed to delete {file}: {ex.Message}");
                    }
                }

                // Try to delete any subdirectories
                var dirs = Directory.GetDirectories(path);
                foreach (var dir in dirs)
                {
                    try
                    {
                        Directory.Delete(dir, true);
                        if (logLevel == LogLevelForFileManagement.All) Console.WriteLine($"Successfully deleted directory {dir}");
                    }
                    catch (Exception ex)
                    {
                        if (logLevel != LogLevelForFileManagement.None) Console.WriteLine($"Failed to delete directory {dir}: {ex.Message}");
                    }
                }

                // Finally try to delete the main directory
                try
                {
                    Directory.Delete(path);
                    if (logLevel == LogLevelForFileManagement.All) Console.WriteLine($"Successfully deleted main directory {path}");
                }
                catch (Exception ex)
                {
                    if (logLevel != LogLevelForFileManagement.None) Console.WriteLine($"Failed to delete main directory: {ex.Message}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in manual cleanup: {ex.Message}");
            }
        }
    }
}
