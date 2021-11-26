using System;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class _Extensions
    {
        public static int FindService<TService>(this IServiceCollection services, int start = 0)
        {
            for (int i = start; i < services.Count; i++)
            {
                var d = services[i];
                if (d.ServiceType == typeof(TService))
                    return i;
            }
            return -1;
        }

        public static bool HasService<TService>(this IServiceCollection services) => services.FindService<TService>() != -1;

        public static bool Replace<TService>(this IServiceCollection services, Func<ServiceDescriptor, ServiceDescriptor> replace)
        {
            for (int i = 0; i < services.Count; i++)
            {
                var d = services[i];
                if (d.ServiceType == typeof(TService))
                {
                    var n = replace(d);
                    if (n != null)
                        services[i] = n;
                    return true;
                }
            }
            return false;
        }

        public static void Remove<TService, TImplementation>(this IServiceCollection services)
        {
            for (int i = services.Count - 1; i >= 0; i--)
            {
                var d = services[i];
                if (d.ServiceType == typeof(TService))
                {
                    if (d.ImplementationType == typeof(TImplementation) ||
                        d.ImplementationInstance is TImplementation)
                        services.RemoveAt(i);
                }
            }
        }

        //public static IServiceCollection AddUpdate<TService>(this IServiceCollection services, Action<TService> update = null)
        //{
        //    return services.AddTransient(new ServiceUpdater<TService>(update).GetService);
        //}
        //public static void InvokeUpdate<TService>(this IServiceProvider services)
        //{
        //    services.InvokeUpdate<TService>(services.GetService<TService>());
        //}
        //public static void InvokeUpdate<TService>(this IServiceProvider services, TService obj)
        //{
        //    if (obj != null)
        //        services.GetService<ServiceUpdater<TService>>()?.Update(obj);
        //}

        //class ServiceUpdater<T>
        //{
        //    public Action<T> Update { get; }
        //    public ServiceUpdater(Action<T> update)
        //    {
        //        Update = update ?? _null.noop<T>;
        //    }

        //    internal ServiceUpdater<T> GetService(IServiceProvider services) => this;
        //}
    }
}