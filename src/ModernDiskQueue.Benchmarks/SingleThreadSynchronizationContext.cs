namespace ModernDiskQueue.Benchmarks
{
    using System.Collections.Concurrent;

    public partial class ThreadsAndTasks
    {
        /// <summary>
        /// SynchronizationContext that runs on a single thread.
        /// This is meant to help with putting async workloads into a dedicated thread.
        /// </summary>
        public class SingleThreadSynchronizationContext : SynchronizationContext
        {
            /// <summary>
            /// Queue of callbacks to be executed on the thread.
            /// </summary>
            private readonly BlockingCollection<(SendOrPostCallback, object?)> _queue = [];

            /// <summary>
            /// Post method that adds the callback to the queue.
            /// </summary>
            public override void Post(SendOrPostCallback d, object? state)
            {
                _queue.Add((d, state));
            }

            /// <summary>
            /// Run the message loop on the current thread.
            /// </summary>
            public void RunOnCurrentThread()
            {
                foreach (var (callback, state) in _queue.GetConsumingEnumerable())
                {
                    callback(state!);
                }
            }

            /// <summary>
            /// Complete the queue and stop processing.
            /// </summary>
            public void Complete() => _queue.CompleteAdding();
        }
    }
}
