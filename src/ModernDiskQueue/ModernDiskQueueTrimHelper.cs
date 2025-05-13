using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ModernDiskQueue
{
    /// <summary>
    /// Helper class to ensure that types are preserved for trimming.
    /// </summary>
    public static class ModernDiskQueueTrimHelper
    {
        /// <summary>
        /// Ensures that types are preserved for trimming.
        /// </summary>
        /// [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(Microsoft.Extensions.Logging.ILogger<>))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(Microsoft.Extensions.Logging.ILoggerFactory))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(Microsoft.Extensions.Logging.Abstractions.NullLogger<>))]
        public static void EnsureTypesArePreserved()
        {
            // Reference but don't execute code that forces the types to be loaded
            var type1 = typeof(Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory);
            var type2 = typeof(Microsoft.Extensions.Logging.Abstractions.NullLogger<>);
            var type3 = typeof(Microsoft.Extensions.Logging.LoggerFactory);
            var type4 = typeof(Microsoft.Extensions.Logging.Logger<>);
        }
    }
}