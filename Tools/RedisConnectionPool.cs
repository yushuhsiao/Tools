using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace StackExchange.Redis
{
    public interface IRedisConnection : IDisposable
    {
        bool IsAlive { get; }
        void CloseConnection();

        Task<T> GetObjectAsync<T>(RedisKey key);
        Task<bool> SetObjectAsync<T>(RedisKey key, T obj, TimeSpan? expiry = null, When when = When.Always);

        RedisValue StringGet(RedisKey key);
        bool StringSet(RedisKey key, RedisValue value, TimeSpan? expiry = null);
        bool KeyDelete(RedisKey key);
        long Publish(RedisChannel channel, RedisValue message, CommandFlags flags = CommandFlags.None);

        Task<bool> KeyExistsAsync(RedisKey key);
        Task<bool> KeyDeleteAsync(RedisKey key);
        Task<List<string>> GetKeys(string host, int db);
        Task<string> StringGetAsync(RedisKey key, CommandFlags flags = CommandFlags.None);
        Task<bool> StringSetAsync(RedisKey key, string value, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None);
        //void Test();
    }

    public static class RedisConnectionExtensions
    {
        public static async Task<IRedisConnection> GetRedisConnectionAsync(this IServiceProvider service, string configuration)
        {
            return await service.GetService<_RedisConnectionPool>().GetConnectionAsync(configuration);
        }

        public static IRedisConnection GetRedisConnection(this IServiceProvider service, string configuration)
        {
            return service.GetService<_RedisConnectionPool>().GetConnection(configuration);
        }

        public static IServiceCollection AddRedisConnectionPool(this IServiceCollection services)
        {
            services.TryAddSingleton<_RedisConnectionPool>();
            return services;
        }

        private static readonly _RedisConnection _null_item = new _RedisConnection(null, null);

        private class _RedisConnectionPool
        {
            public ILogger _logger;
            private List<_RedisConnection> _items = new List<_RedisConnection>();

            /// <summary>
            /// Cache Timeout, 0:disabled
            /// </summary>
            public double ObjectTimeout { get; set; } = 300;//5 * 60;

            public _RedisConnectionPool(ILogger<_RedisConnectionPool> logger)
            {
                _logger = logger;
            }

            public async Task<_RedisConnection> GetConnectionAsync(string configuration)
            {
                if (this.GetFromPool(configuration, out var conn))
                    return await Task.FromResult(conn);

                if (!string.IsNullOrEmpty(configuration))
                {
                    try
                    {
                        var multiplexer = await ConnectionMultiplexer.ConnectAsync(configuration);
                        return new _RedisConnection(this, multiplexer)
                        {
                            configuration = configuration
                        };
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"Failed to connect redis : {configuration}.");
                    }
                }
                return await Task.FromResult(_null_item);
            }

            public _RedisConnection GetConnection(string configuration)
            {
                if (this.GetFromPool(configuration, out var conn))
                    return conn;

                if (!string.IsNullOrEmpty(configuration))
                {
                    try
                    {
                        var multiplexer = ConnectionMultiplexer.Connect(configuration);
                        return new _RedisConnection(this, multiplexer)
                        {
                            configuration = configuration
                        };
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"Failed to connect redis : {configuration}.");
                    }
                }
                return _null_item;
            }

            private bool GetFromPool(string configuration, out _RedisConnection result)
            {
                lock (_items)
                {
                    for (int i = _items.Count - 1; i >= 0; i--)
                    {
                        var _item = _items[i];
                        if (_item.IsObjectTimeout(ObjectTimeout))
                        {
                            _item.CloseConnection();
                            _items.RemoveAt(i);
                        }
                        else if (configuration == _item.configuration)
                        {
                            _items.RemoveAt(i);
                            result = _item;
                            return true;
                        }
                    }
                }
                result = null;
                return false;
            }

            public void ReturnToPool(_RedisConnection conn)
            {
                if (conn.IsAlive)
                    lock (_items)
                        _items.Add(conn);
            }
        }

        private class _RedisConnection : IRedisConnection, IDisposable
        {
            private _RedisConnectionPool _owner;
            public ConnectionMultiplexer Multiplexer { get; private set; }
            public bool IsAlive => false == object.ReferenceEquals(this, _null_item) && Multiplexer != null;
            private IDatabase _database;
            public DateTime _objectTime = DateTime.Now;
            public string configuration;

            public _RedisConnection(_RedisConnectionPool owner, ConnectionMultiplexer multiplexer)
            {
                _owner = owner;
                Multiplexer = multiplexer;
                _database = multiplexer?.GetDatabase();
            }

            public bool IsObjectTimeout(double timeout)
            {
                TimeSpan t = DateTime.Now - this._objectTime;
                return t.TotalSeconds >= timeout;
            }

            public void CloseConnection(Exception ex, string msg, [CallerMemberName] string caller = null)
            {
                CloseConnection();
                _owner._logger.LogError(ex, $"Error : {caller}({msg})");
            }

            public void CloseConnection()
            {
                using (var m = Multiplexer)
                {
                    Multiplexer = null;
                    _database = null;
                }
            }

            void IDisposable.Dispose() => _owner?.ReturnToPool(this);

            //public void Test()
            //{
            //    var nn = _database.ListRange("a");
            //    var n = _database.ListLeftPush("a", "1");
            //    n = _database.ListLeftPush("a", "2");
            //    n = _database.ListLeftPush("a", "3");
            //}

            public async Task<T> GetObjectAsync<T>(RedisKey key)
            {
                if (this.IsAlive)
                {
                    try
                    {
                        RedisValue value = await this._database.StringGetAsync(key);
                        if (value.HasValue)
                        {
                            try
                            {
                                return JsonConvert.DeserializeObject<T>(value.ToString());
                            }
                            catch (Exception ex)
                            {
                                _owner._logger.LogError(ex, "Deserialize error.");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        CloseConnection();
                        _owner._logger.LogError(ex, $"Error : StringGet({key})");
                    }
                }
                return await Task.FromResult<T>(default(T));
            }

            public async Task<bool> SetObjectAsync<T>(RedisKey key, T obj, TimeSpan? expiry = null, When when = When.Always)
            {
                if (this.IsAlive)
                {
                    RedisValue value = JsonConvert.SerializeObject(obj);
                    try
                    {
                        return await this._database.StringSetAsync(key, value, expiry: expiry, when: when);
                    }
                    catch (Exception ex)
                    {
                        CloseConnection();
                        _owner._logger.LogError(ex, $"Error : StringSet({key})");
                    }
                }
                return await Task.FromResult(false);
            }

            public async Task<string> StringGetAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
            {
                if (this.IsAlive)
                {
                    try
                    {
                        return await this._database.StringGetAsync(key, flags);
                    }
                    catch (Exception ex)
                    {
                        CloseConnection();
                        _owner._logger.LogError(ex, $"Error : StringGet({key})");
                    }
                }
                return await Task.FromResult(default(string));
            }

            public async Task<bool> StringSetAsync(RedisKey key, string value, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
            {
                if (this.IsAlive)
                {
                    try
                    {
                        return await this._database.StringSetAsync(key, value, expiry: expiry, when: when);
                    }
                    catch (Exception ex)
                    {
                        CloseConnection();
                        _owner._logger.LogError(ex, $"Error : StringSet({key})");
                    }
                }
                return await Task.FromResult(false);
            }

            public async Task<bool> KeyExistsAsync(RedisKey key)
            {
                if (this.IsAlive)
                {
                    try
                    {
                        return await this._database.KeyExistsAsync(key);
                    }
                    catch (Exception ex)
                    {
                        CloseConnection();
                        _owner._logger.LogError(ex, $"Error : KeyExists({key})");
                    }
                }
                return await Task.FromResult(false);
            }

            public async Task<bool> KeyDeleteAsync(RedisKey key)
            {
                if (this.IsAlive)
                {
                    try
                    {
                        return await this._database.KeyDeleteAsync(key);
                    }
                    catch (Exception ex)
                    {
                        CloseConnection();
                        _owner._logger.LogError(ex, $"Error : KeyDelete({key})");
                    }
                }
                return await Task.FromResult(false);
            }

            public async Task<List<string>> GetKeys(string host,int db)
            {
                if (this.IsAlive)
                {
                    try
                    {
                        List<string> endmodel = new List<string>();
                        var multiplexer = await ConnectionMultiplexer.ConnectAsync(configuration);
                        var server = multiplexer.GetServer(host);

                        var aaa = server.Keys(db, "*");

                        foreach (var m in aaa)
                            endmodel.Add(m);

                        return endmodel;
                    }
                    catch (Exception ex)
                    {
                        CloseConnection();
                        _owner._logger.LogError(ex, $"Error : GetKeys");
                    }
                }
                return null;
            }



            public RedisValue StringGet(RedisKey key)
            {
                if (this.IsAlive)
                {
                    try { return this._database.StringGet(key); }
                    catch (Exception ex) { CloseConnection(ex, key); }
                }
                return default(RedisValue);
            }

            public bool StringSet(RedisKey key, RedisValue value, TimeSpan? expiry = null)
            {
                if (this.IsAlive)
                {
                    try { return this._database.StringSet(key, value, expiry: expiry); }
                    catch (Exception ex) { CloseConnection(ex, $"{key}"); }
                }
                return false;
            }

            public bool KeyDelete(RedisKey key)
            {
                if (this.IsAlive)
                {
                    try { return this._database.KeyDelete(key); }
                    catch (Exception ex) { CloseConnection(ex, $"{key}"); }
                }
                return false;
            }

            public long Publish(RedisChannel channel, RedisValue message, CommandFlags flags = CommandFlags.None)
            {
                if (this.IsAlive)
                {
                    try { return this._database.Publish(channel, message, flags); }
                    catch (Exception ex) { CloseConnection(ex, $"{channel}"); }
                }
                return 0;
            }
        }
    }
}