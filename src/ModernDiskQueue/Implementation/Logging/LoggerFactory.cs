namespace ModernDiskQueue.Implementation.Logging
{
    using Microsoft.Extensions.Logging;
    using Serilog.Extensions.Logging;

    internal static class LoggerFactory
    {
        public static ILoggerFactory Factory { get; } = new SerilogLoggerFactory(MdqLogger.Logger);
    }
}
