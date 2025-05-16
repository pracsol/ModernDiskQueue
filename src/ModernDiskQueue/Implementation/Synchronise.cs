
namespace ModernDiskQueue.Implementation
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Helper class to properly wait for async tasks
    /// </summary>
    internal static class Synchronise
    {
        private static readonly TaskFactory _taskFactory = new(CancellationToken.None,
                TaskCreationOptions.None,
                TaskContinuationOptions.None,
                TaskScheduler.Default);

        /// <summary>
        /// Run an async function synchronously and return the result
        /// </summary>
        public static TResult Run<TResult>(Func<Task<TResult>> func)
        {
            if (_taskFactory == null) throw new Exception("Static init failed");
            ArgumentNullException.ThrowIfNull(func);

            var rawTask = _taskFactory.StartNew(func).Unwrap();
            if (rawTask == null) throw new Exception("Invalid task");

            return rawTask.GetAwaiter().GetResult();
        }

        /// <summary>
        /// Run an async action synchronously
        /// </summary>
        public static void Run(Func<Task> func)
        {
            ArgumentNullException.ThrowIfNull(func);

            var rawTask = _taskFactory.StartNew(func).Unwrap();
            if (rawTask == null) throw new Exception("Invalid task");

            rawTask.GetAwaiter().GetResult();
        }
    }
}