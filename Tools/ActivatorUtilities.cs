using Microsoft.Extensions.DependencyInjection;

namespace System
{
    [System.Diagnostics.DebuggerStepThrough]
    public static class DIActivatorUtilities
    {
        public static object CreateInstance(this IServiceProvider provider, Type instanceType, params object[] parameters)
            => ActivatorUtilities.CreateInstance(provider, instanceType, parameters);
        
        public static T CreateInstance<T>(this IServiceProvider provider)
            => ActivatorUtilities.CreateInstance<T>(provider);

        public static T CreateInstance<T>(this IServiceProvider provider, params object[] parameters)
            => ActivatorUtilities.CreateInstance<T>(provider, parameters);

        public static object GetServiceOrCreateInstance(this IServiceProvider provider, Type type)
            => ActivatorUtilities.GetServiceOrCreateInstance(provider, type);

        public static T GetServiceOrCreateInstance<T>(this IServiceProvider provider)
            => ActivatorUtilities.GetServiceOrCreateInstance<T>(provider);
        }
    }
