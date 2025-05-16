using BenchmarkDotNet.Running;

namespace ModernDiskQueue.Benchmarks
{
    public class Program
    {
        public static void Main(string[] args)
        {
            BenchmarkRunner.Run(new[]
            {
                //typeof(ContentiousEnqueues),
                //typeof(HighVolumeEnqueues),
                typeof(ThreadsAndTasks),
            });
        }
    }
}
