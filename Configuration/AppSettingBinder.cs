using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Primitives;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Microsoft.Extensions.Configuration
{
    public interface IConfiguration<TCallerType> : IConfiguration { }

    public static class AppSettingBinder
    {
        public static IServiceCollection AddConfigurationBinder(this IServiceCollection service)
        {
            service.TryAddSingleton(typeof(IConfiguration<>), typeof(_Binder<>));
            return service;
        }

        public static IConfiguration GetConfiguration(this IServiceProvider service, Type callerType)
        {
            return (IConfiguration)service.GetService(typeof(IConfiguration<>).MakeGenericType(callerType));
        }
        public static IConfiguration GetConfiguration<TCallerType>(this IServiceProvider service)
        {
            return (IConfiguration)service.GetService(typeof(IConfiguration<>).MakeGenericType(typeof(TCallerType)));
        }


        public static TValue GetValue<TValue>(this IConfiguration configuration, [CallerMemberName] string name = null, int index1 = 0, int index2 = 0)
        {
            if (configuration is _Binder binder)
                return binder.GetValue<TValue>(name, index1, index2);
            return ConfigurationBinder.GetValue(configuration, name, default(TValue));
        }

        public static void SetValue<TValue>(this IConfiguration configuration, TValue value, [CallerMemberName] string name = null, int index1 = 0, int index2 = 0)
        {
            if (configuration is _Binder binder)
                binder.SetValue(value, name, index1, index2);
        }

        internal interface ISource : IConfigurationSource
        {
            Provider Provider { get; }
        }

        public abstract class Source<T> : ISource where T : Provider, new()
        {
            internal T Provider => new T();

            Provider ISource.Provider => Provider;

            public virtual void OnBuild(T provider) { }

            IConfigurationProvider IConfigurationSource.Build(IConfigurationBuilder builder)
            {
                OnBuild(Provider);
                return Provider;
            }
        }

        public abstract class Provider : ConfigurationProvider
        {
            public abstract void OnInit(IServiceProvider service);
        }

        public static string BuildKey(string _section, string _key, int index1, int index2)
        {
            string key;
            if (string.IsNullOrEmpty(_section))
            {
                if (index1 == 0 && index2 == 0)
                    key = _key;
                else
                    key = $"{_key}:{index1}:{index2}";
            }
            else if (index1 == 0 && index2 == 0)
                key = $"{_section}:{_key}";
            else
                key = $"{_section}:{_key}:{index1}:{index2}";
            return key;
        }

        private abstract class _Binder
        {
            private class _BinderMember
            {
                public AppSettingAttribute src;
                public MemberInfo Member;
                public DefaultValueAttribute DefaultValue;
                //public AppSettingProviderAttribute[] Providers = Array.Empty<AppSettingProviderAttribute>();

                private Dictionary<Type, object> _defaultValues = new Dictionary<Type, object>();

                public TValue GetDefaultValue<TValue>()
                {
                    if (DefaultValue != null)
                    {
                        try
                        {
                            lock (_defaultValues)
                            {
                                if (_defaultValues.TryGetValue(typeof(TValue), out object tmp))
                                    return (TValue)tmp;
                                TValue result = DefaultValue.GetValue<TValue>();
                                _defaultValues[typeof(TValue)] = result;
                                return result;
                            }
                        }
                        catch { }
                    }
                    return default(TValue);
                }
            }

            protected IServiceProvider _service;
            protected IConfiguration _configuration;
            private _BinderMember[] _members;

            public _Binder(IServiceProvider service, IConfiguration configuration)
            {
                _service = service;
                _configuration = configuration;

                #region init
                if (configuration is IConfigurationRoot configRoot)
                {
                    foreach (var provider in configRoot.Providers)
                        if (provider is Provider _provider)
                            _provider.OnInit(service);
                }
                #endregion

                #region find members

                var m1 = this.CallerType.GetMembers(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance);
                var members = new List<_BinderMember>();
                foreach (MemberInfo m in m1)
                {
                    var attr1 = m.GetCustomAttribute<AppSettingAttribute>();
                    //var attr2 = m.GetCustomAttributes<AppSettingProviderAttribute>();
                    if (attr1 != null)
                    {
                        var binderMember = new _BinderMember()
                        {
                            src = attr1,
                            Member = m,
                            DefaultValue = m.GetCustomAttribute<DefaultValueAttribute>()
                        };
                        //List<AppSettingProviderAttribute> providers = null;
                        //foreach (var attr2_ in attr2)
                        //{
                        //    if (attr2_.Type.IsSubclassOf<AppSettingBinder.Provider>())
                        //    {
                        //        if (providers == null)
                        //            providers = new List<AppSettingProviderAttribute>();
                        //        providers.Add(attr2_);
                        //    }
                        //}
                        //if (providers != null)
                        //    binderMember.Providers = providers.ToArray();
                        members.Add(binderMember);
                    }
                }
                this._members = members.ToArray();

                #endregion
            }

            protected abstract Type CallerType { get; }

            private string BuildKey(_BinderMember item, int index1, int index2)
            {
                string _section = item.src.SectionName;
                string _key = item.src.Key ?? item.Member.Name;
                return AppSettingBinder.BuildKey(_section, _key, index1, index2);
            }

            public TValue GetValue<TValue>(string name, int index1, int index2)
            {
                if (name == null)
                    return default;

                for (int i = 0; i < _members.Length; i++)
                {
                    _BinderMember item = _members[i];
                    if (item.Member.Name == name)
                    {
                        string key = BuildKey(item, index1, index2);
                        TValue defaultValue = item.GetDefaultValue<TValue>();
                        return _configuration.GetValue(key, defaultValue);
                    }
                }
                return default;
            }

            public virtual void SetValue<TValue>(TValue value, string name, int index1, int index2)
            {
                if (name == null)
                    return;
                if (_configuration is ConfigurationManager manager)
                {
                    for (int i = 0; i < _members.Length; i++)
                    {
                        _BinderMember item = _members[i];
                        if (item.Member.Name == name)
                        {
                            //var enableWrite = Guid.Empty;

                            //foreach (var p1 in item.Providers)
                            //{
                            //    foreach (var p2a in manager.Sources)
                            //    {
                            //        if (p2a is AppSettingBinder.Source p2)
                            //        {
                            //            if (p1.Type == p2.Provider.GetType())
                            //            {
                            //                if (enableWrite == Guid.Empty)
                            //                    enableWrite = Guid.NewGuid();
                            //                ;
                            //            }
                            //        }
                            //    }
                            //}

                            string key = BuildKey(item, index1, index2);
                            if (value == null)
                                manager[key] = null;
                            else
                                manager[key] = Convert.ToString(value);
                        }
                    }
                }
            }
        }

        [DebuggerStepThrough]
        private class _Binder<TCallerType> : _Binder, IConfiguration<TCallerType>
        {
            public _Binder(IServiceProvider service, IConfiguration configuration)
                : base(service, configuration)
            {
            }

            protected override Type CallerType => typeof(TCallerType);

            string IConfiguration.this[string key]
            {
                get => _configuration[key];
                set => _configuration[key] = value;
            }

            IConfigurationSection IConfiguration.GetSection(string key) => _configuration.GetSection(key);

            IEnumerable<IConfigurationSection> IConfiguration.GetChildren() => _configuration.GetChildren();

            IChangeToken IConfiguration.GetReloadToken() => _configuration.GetReloadToken();
        }
    }
}