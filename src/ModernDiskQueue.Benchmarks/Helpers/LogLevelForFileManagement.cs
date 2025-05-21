// <copyright file="LogLevelForFileManagement.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>

namespace ModernDiskQueue.Benchmarks.Helpers
{
    /// <summary>
    /// Defines the logging levels for file cleanup operations.
    /// </summary>
    internal enum LogLevelForFileManagement
    {
        /// <summary>
        /// No console output will be written.
        /// </summary>
        None = 1,

        /// <summary>
        /// Only the number of files will be logged.
        /// </summary>
        QuantityOnly = 2,

        /// <summary>
        /// List of files deleted will be logged.
        /// </summary>
        All = 3,
    }
}
