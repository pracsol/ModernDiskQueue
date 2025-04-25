namespace ModernDiskQueue.Implementation.Logging
{
    using Microsoft.Extensions.Logging;
    using Serilog.Extensions.Logging;

    internal static class LoggerAdapter
    {
        public static ILogger<T> CreateLogger<T>() => LoggerFactory.Factory.CreateLogger<T>();
    }
}
