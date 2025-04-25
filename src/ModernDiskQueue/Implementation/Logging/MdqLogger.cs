namespace ModernDiskQueue.Implementation.Logging
{
    using Serilog;

    internal static class MdqLogger
    {
        public static ILogger Logger { get; private set; }

        static MdqLogger()
        {
            Logger = new LoggerConfiguration()
                .MinimumLevel.Warning()
                .Enrich.WithThreadId()
                .Enrich.WithThreadName()
                //.WriteTo.Console(outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] [TID:{ThreadId}] {Message:lj} <TName:{ThreadName}>{NewLine}{Exception}")
                //.WriteTo.Debug()
                .WriteTo.File("log/log.txt",
                    fileSizeLimitBytes: 50000000,
                    rollOnFileSizeLimit:true,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] [TID:{ThreadId}] {Message:lj} <TName:{ThreadName}>{NewLine}{Exception}",
                    retainedFileCountLimit: 300)
                .CreateLogger();
        }
    }
}
