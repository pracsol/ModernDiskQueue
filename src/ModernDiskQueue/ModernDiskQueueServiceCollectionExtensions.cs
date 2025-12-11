namespace ModernDiskQueue
{
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.DependencyInjection.Extensions;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.Abstractions;
    using ModernDiskQueue.Implementation;
    using ModernDiskQueue.Implementation.Interfaces;
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;

    /// <summary>
    /// Extension methods for <see cref="IServiceCollection"/> to add ModernDiskQueue services.
    /// </summary>
    public static class ModernDiskQueueServiceCollectionExtensions
    {
        /// <summary>
        /// Adds ModernDiskQueue services to the specified <see cref="IServiceCollection"/> with the specified options.
        /// </summary>
        /// <param name="services">Implementation of <see cref="IServiceCollection"/></param>
        /// <param name="configure">Default options.</param>
        /// <returns></returns>
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(ILogger<>))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(ILoggerFactory))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(ModernDiskQueueOptions))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(StandardFileDriver))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(PersistentQueueFactory))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(IPersistentQueueFactory))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(IFileDriver))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(IServiceCollection))]
        public static IServiceCollection AddModernDiskQueue(
            this IServiceCollection services,
            Action<ModernDiskQueueOptions>? configure = null)
        {
            if (configure != null)
            {
                services.Configure(configure);
            }

            // Add NullLoggerFactory if ILoggerFactory isn't already registered
            if (!services.Any(sd => sd.ServiceType == typeof(ILoggerFactory)))
            {
                services.AddSingleton<ILoggerFactory>(NullLoggerFactory.Instance);
            }

            // Register IFileDriver - consumers can replace this before calling AddModernDiskQueue
            // or by calling services.Replace() afterward
            services.TryAddSingleton<IFileDriver, StandardFileDriver>();

            // Register services
            services.AddSingleton<IPersistentQueueFactory, PersistentQueueFactory>();

            return services;
        }
    }
}
