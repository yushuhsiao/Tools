using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace StackExchange.Redis
{
    public sealed class RedisConnection : RedisConnectionExtensions.RedisConnectionBase, IDisposable
    {
        internal static readonly RedisConnection _null_item = new RedisConnection(null, null, null, 0);

        public bool IsAlive => false == object.ReferenceEquals(this, _null_item) && _database != null;
        private IDatabase _database;

        internal RedisConnection(IServiceProvider service, IDatabase database, string configuration, double timeout)
            : base(configuration, timeout)
        {
            this._database = database;
            this._logger = service?.GetService<ILogger<RedisConnection>>();
        }


        internal void CloseConnection(Exception ex, string msg, [CallerMemberName] string caller = null)
        {
            using (this)
                _logger.LogError(ex, $"Error : {caller}({msg})");
        }

        public void CloseConnection()
        {
            using (this)
                return;
        }

        void IDisposable.Dispose()
        {
            using (var m = _database?.Multiplexer)
                _database = null;
        }



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
                            _logger.LogError(ex, "Deserialize error.");
                        }
                    }
                }
                catch (Exception ex)
                {
                    CloseConnection();
                    _logger.LogError(ex, $"Error : StringGet({key})");
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
                    _logger.LogError(ex, $"Error : StringSet({key})");
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
                    _logger.LogError(ex, $"Error : StringGet({key})");
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
                    _logger.LogError(ex, $"Error : StringSet({key})");
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
                    _logger.LogError(ex, $"Error : KeyExists({key})");
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
                    _logger.LogError(ex, $"Error : KeyDelete({key})");
                }
            }
            return await Task.FromResult(false);
        }

        public async Task<List<string>> GetKeys(string host, int db)
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
                    _logger.LogError(ex, $"Error : GetKeys");
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