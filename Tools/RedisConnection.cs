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

        private IDatabase _database;

        internal RedisConnection(IServiceProvider service, IDatabase database, string configuration, double timeout)
            : base(service?.GetService<ILogger<RedisConnection>>(), configuration, timeout)
        {
            this._database = database;
        }


        internal void CloseConnection(Exception ex, string msg, [CallerMemberName] string caller = null)
        {
            using (this)
                logger.LogError(ex, $"Error : {caller}({msg})");
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

        public bool IsAlive => this._GetDatabase(out var database);

        private bool _GetDatabase(out IDatabase database)
        {
            database = null;
            if (object.ReferenceEquals(this, _null_item))
                return false;
            database = _database;
            return database != null;
        }



        public async Task<T> GetObjectAsync<T>(RedisKey key)
        {
            if (this._GetDatabase(out var database))
            {
                try
                {
                    RedisValue value = await database.StringGetAsync(key);
                    if (value.HasValue)
                    {
                        try
                        {
                            return JsonConvert.DeserializeObject<T>(value.ToString());
                        }
                        catch (Exception ex)
                        {
                            logger.LogError(ex, "Deserialize error.");
                        }
                    }
                }
                catch (Exception ex)
                {
                    CloseConnection();
                    logger.LogError(ex, $"Error : StringGet({key})");
                }
            }
            return await Task.FromResult<T>(default(T));
        }

        public async Task<bool> SetObjectAsync<T>(RedisKey key, T obj, TimeSpan? expiry = null, When when = When.Always)
        {
            if (this._GetDatabase(out var database))
            {
                RedisValue value = JsonConvert.SerializeObject(obj);
                try
                {
                    return await database.StringSetAsync(key, value, expiry: expiry, when: when);
                }
                catch (Exception ex)
                {
                    CloseConnection();
                    logger.LogError(ex, $"Error : StringSet({key})");
                }
            }
            return await Task.FromResult(false);
        }

        public async Task<string> StringGetAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try
                {
                    return await database.StringGetAsync(key, flags);
                }
                catch (Exception ex)
                {
                    CloseConnection();
                    logger.LogError(ex, $"Error : StringGet({key})");
                }
            }
            return await Task.FromResult(default(string));
        }

        public async Task<bool> StringSetAsync(RedisKey key, string value, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try
                {
                    return await database.StringSetAsync(key, value, expiry: expiry, when: when);
                }
                catch (Exception ex)
                {
                    CloseConnection();
                    logger.LogError(ex, $"Error : StringSet({key})");
                }
            }
            return await Task.FromResult(false);
        }

        public async Task<bool> KeyExistsAsync(RedisKey key)
        {
            if (this._GetDatabase(out var database))
            {
                try
                {
                    return await database.KeyExistsAsync(key);
                }
                catch (Exception ex)
                {
                    CloseConnection();
                    logger.LogError(ex, $"Error : KeyExists({key})");
                }
            }
            return await Task.FromResult(false);
        }

        public async Task<bool> KeyDeleteAsync(RedisKey key)
        {
            if (this._GetDatabase(out var database))
            {
                try
                {
                    return await database.KeyDeleteAsync(key);
                }
                catch (Exception ex)
                {
                    CloseConnection();
                    logger.LogError(ex, $"Error : KeyDelete({key})");
                }
            }
            return await Task.FromResult(false);
        }

        public async Task<List<string>> GetKeys(int db, string pattern = "*")
        {
            List<string> endmodel = null;
            if (this._GetDatabase(out var database))
            {
                try
                {
                    if (database.Database != db)
                        database = database.Multiplexer.GetDatabase(db);

                    endmodel = new List<string>();
                    var endPoints = database.Multiplexer.GetEndPoints();
                    if (endPoints.Length > 0)
                    {
                        var server = database.Multiplexer.GetServer(endPoints[0]);

                        var keys = server.Keys(db, "*");

                        foreach (var m in keys)
                            endmodel.Add(m);
                    }
                    return endmodel;
                }
                catch (Exception ex)
                {
                    CloseConnection();
                    logger.LogError(ex, $"Error : GetKeys");
                }
            }
            return await Task.FromResult(endmodel);
        }



        public RedisValue StringGet(RedisKey key)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringGet(key); }
                catch (Exception ex) { CloseConnection(ex, key); }
            }
            return default(RedisValue);
        }

        public bool StringSet(RedisKey key, RedisValue value, TimeSpan? expiry = null)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringSet(key, value, expiry: expiry); }
                catch (Exception ex) { CloseConnection(ex, $"{key}"); }
            }
            return false;
        }

        public bool KeyDelete(RedisKey key)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.KeyDelete(key); }
                catch (Exception ex) { CloseConnection(ex, $"{key}"); }
            }
            return false;
        }

        public long Publish(RedisChannel channel, RedisValue message, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.Publish(channel, message, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{channel}"); }
            }
            return 0;
        }
    }
}