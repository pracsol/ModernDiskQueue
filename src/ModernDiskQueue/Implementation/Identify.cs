using System.Diagnostics;

namespace ModernDiskQueue.Implementation
{
    internal static class Identify
    {
        public static long Thread()
        {
            return ((long)System.Threading.Thread.CurrentThread.ManagedThreadId << 32) + Process.GetCurrentProcess().Id;
        }
    }
}