using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace StackExchange.Redis
{
    public static class RedisConnectionExtensions
    {
        private const double DefaultTimeout = 5 * 60 * 1000;
        private const double SubscriberTimeout = 3 * 10 * 1000;

        public static IServiceCollection AddRedisConnectionPool(this IServiceCollection services)
        {
            services.TryAddSingleton<_RedisConnectionPool>();
            return services;
        }



        public static RedisConnection GetRedisConnection(this IServiceProvider service, string configuration, double timeout = DefaultTimeout)
        {
            return service.GetService<_RedisConnectionPool>().GetConnection(configuration, timeout);
        }

        public static async Task<RedisConnection> GetRedisConnectionAsync(this IServiceProvider service, string configuration, double timeout = DefaultTimeout)
        {
            var redis = await service.GetService<_RedisConnectionPool>().GetConnectionAsync(configuration, timeout);

            //foreach (var n in redis.Info_Keyspace())
            //{
            //    ;
            //}

            return redis;
        }

        public static RedisSubscriber GetRedisSubscriber(this IServiceProvider service, string configuration, double timeout = SubscriberTimeout)
        {
            return service.GetService<_RedisConnectionPool>().GetSubscriber(configuration, timeout);
        }

        public static async Task<RedisSubscriber> GetRedisSubscriberAsync(this IServiceProvider service, string configuration, double timeout = SubscriberTimeout)
        {
            return await service.GetService<_RedisConnectionPool>().GetSubscriberAsync(configuration, timeout);
        }


        internal class _RedisConnectionPool
        {
            private IServiceProvider _service;
            private ILogger _logger;
            private List<RedisConnection> _connections = new List<RedisConnection>();
            private List<RedisSubscriber> _subscribers = new List<RedisSubscriber>();

            public _RedisConnectionPool(IServiceProvider service, ILogger<_RedisConnectionPool> logger)
            {
                _service = service;
                _logger = logger;
            }

            public RedisConnection GetConnection(string configuration, double timeout)
            {
                if (this.GetConnection(configuration, timeout, out var conn))
                    return conn;

                if (!string.IsNullOrEmpty(configuration))
                {
                    try
                    {
                        return new RedisConnection(_service, this.ReleaseConnection,
                            ConnectionMultiplexer.Connect(configuration)?.GetDatabase(asyncState: _connections),
                            configuration,
                            timeout);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"Failed to connect redis : {configuration}.");
                    }
                }
                return RedisConnection._null_item;
            }

            public async Task<RedisConnection> GetConnectionAsync(string configuration, double timeout)
            {
                if (this.GetConnection(configuration, timeout, out var conn))
                    return await Task.FromResult(conn);

                if (!string.IsNullOrEmpty(configuration))
                {
                    try
                    {
                        return new RedisConnection(_service, this.ReleaseConnection,
                            (await ConnectionMultiplexer.ConnectAsync(configuration))?.GetDatabase(asyncState: _connections),
                            configuration,
                            timeout);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"Failed to connect redis : {configuration}.");
                    }
                }
                return await Task.FromResult(RedisConnection._null_item);
            }

            public RedisSubscriber GetSubscriber(string configuration, double timeout)
            {
                if (GetSubscriber(configuration, timeout, out var subscriber))
                    return subscriber;

                if (!string.IsNullOrEmpty(configuration))
                {
                    try
                    {
                        subscriber = new RedisSubscriber(_service, this.ReleaseConnection,
                            ConnectionMultiplexer.Connect(configuration)?.GetSubscriber(asyncState: _subscribers),
                            configuration,
                            timeout);
                        lock (_subscribers)
                            _subscribers.Add(subscriber);
                        return subscriber;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"Failed to connect redis : {configuration}.");
                    }
                }

                return null;
            }

            public async Task<RedisSubscriber> GetSubscriberAsync(string configuration, double timeout)
            {
                if (GetSubscriber(configuration, timeout, out var subscriber))
                    return await Task.FromResult(subscriber);

                if (!string.IsNullOrEmpty(configuration))
                {
                    try
                    {
                        subscriber = new RedisSubscriber(_service, this.ReleaseConnection,
                            (await ConnectionMultiplexer.ConnectAsync(configuration))?.GetSubscriber(asyncState: _subscribers),
                            configuration,
                            timeout);
                        lock (_subscribers)
                            _subscribers.Add(subscriber);
                        return subscriber;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"Failed to connect redis : {configuration}.");
                    }
                }

                return null;
            }

            private bool GetSubscriber(string configuration, double timeout, out RedisSubscriber result)
            {
                result = null;
                lock (_subscribers)
                {
                    for (int i = _subscribers.Count - 1; i >= 0; i--)
                    {
                        var _item = _subscribers[i];
                        if (_item.IsAlive == false)
                            _subscribers.RemoveAt(i);
                        else if (configuration == _item.configuration && timeout == _item.Timeout)
                            result = result ?? _item;
                    }
                }
                return result != null;
            }

            private void ReleaseConnection(RedisSubscriber subscriber)
            {
                if (subscriber.IsAlive)
                    lock (_subscribers)
                        _subscribers.Add(subscriber);
            }

            private bool GetConnection(string configuration, double timeout, out RedisConnection result)
            {
                result = null;
                lock (_connections)
                {
                    for (int i = _connections.Count - 1; i >= 0; i--)
                    {
                        var _item = _connections[i];
                        if (_item.IsAlive == false)
                        {
                            using (_item)
                                _connections.RemoveAt(i);
                        }
                        if (_item.IsObjectTimeout())
                        {
                            using (_item)
                                _connections.RemoveAt(i);
                        }
                        else if (configuration == _item.configuration && timeout == _item.Timeout)
                        {
                            result = result ?? _item;
                        }
                    }
                }
                return result != null;
            }

            private void ReleaseConnection(RedisConnection connection)
            {
                if (connection.IsAlive)
                    lock (_connections)
                        _connections.Add(connection);
            }
        }

        public abstract class RedisConnectionBase
        {
            internal readonly string configuration;
            protected TimeCounter timer = new TimeCounter();
            public double Timeout { get; }
            //private readonly DateTime _objectTime = DateTime.Now;
            //public double TimeElapsed => (DateTime.Now - this._objectTime).TotalMilliseconds;
            public bool IsObjectTimeout(bool reset = false) => this.timer.IsTimeout(Timeout, reset);
            protected readonly ILogger logger;

            public RedisConnectionBase(ILogger logger, string configuration, double timeout)
            {
                this.logger = logger;
                this.configuration = configuration;
                this.Timeout = timeout;
            }
        }
    }
}