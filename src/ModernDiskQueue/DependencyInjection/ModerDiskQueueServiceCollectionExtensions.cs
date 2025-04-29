using Microsoft.Extensions.DependencyInjection;
using ModernDiskQueue.Implementation;
using ModernDiskQueue.PublicInterfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ModernDiskQueue.DependencyInjection
{
    /// <summary>
    /// Extension methods for <see cref="IServiceCollection"/> to add ModernDiskQueue services.
    /// </summary>
    public static class ModerDiskQueueServiceCollectionExtensions
    {
        /// <summary>
        /// Adds ModernDiskQueue services to the specified <see cref="IServiceCollection"/> with the specified options.
        /// </summary>
        /// <param name="services">Implementation of <see cref="IServiceCollection"/></param>
        /// <param name="configure">Default options.</param>
        /// <returns></returns>
        public static IServiceCollection AddModernDiskQueue(
            this IServiceCollection services,
            Action<ModernDiskQueueOptions>? configure = null)
        {
            if (configure != null)
            {
                services.Configure(configure);
            }
            else
            {
                services.Configure<ModernDiskQueueOptions>(_ => { });
            }
            
            // Register services
            services.AddSingleton<IPersistentQueueFactory, PersistentQueueFactory>();
            services.AddSingleton<IFileDriver, StandardFileDriver>();
            return services;
        }
    }
}
