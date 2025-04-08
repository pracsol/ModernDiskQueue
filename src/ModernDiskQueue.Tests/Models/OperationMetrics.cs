namespace ModernDiskQueue.Tests.Models
{
    using System;

    public class OperationMetrics
    {
        public DateTime Time { get; set; }

        public string Operation { get; set; } = string.Empty;

        public TimeSpan QueueCreateTime { get; set; }

        public TimeSpan SessionTime { get; set; }

        public TimeSpan OperationTime { get; set; }

        public TimeSpan FlushTime { get; set; }

        public int ThreadId { get; set; }

        public int ItemNumber { get; set; }
    }
}
