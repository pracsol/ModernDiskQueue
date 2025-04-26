using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ModernDiskQueue.Implementation.Logging
{
    internal static class LoggerHelpers
    {
        public static ILogger<T> CreateLoggerFor<T>(ILoggerFactory factory)
        {
            return factory?.CreateLogger<T>() ?? NullLogger<T>.Instance;
        }
    }
}
