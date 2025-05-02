using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.Net;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace StackExchange.Redis
{
    public sealed class RedisConnection : RedisConnectionExtensions.RedisConnectionBase, IDisposable, IDatabase
    {
        internal static readonly RedisConnection _null_item = new RedisConnection(null, null, null, null, 0);

        private IDatabase _database;
        private Action<RedisConnection> _dispose;

        internal RedisConnection(IServiceProvider service, Action<RedisConnection> dispose, IDatabase database, string configuration, double timeout)
            : base(service?.GetService<ILogger<RedisConnection>>(), configuration, timeout)
        {
            this._database = database;
            this._dispose = dispose;
        }


        internal void CloseConnection(Exception ex, string msg, [CallerMemberName] string caller = null)
        {
            logger.LogError(ex, $"Error : {caller}({msg})");
            this.CloseConnection();
        }

        public void CloseConnection()
        {
            using (var m = _database?.Multiplexer)
                _database = null;
            _dispose?.Invoke(this);
        }

        void IDisposable.Dispose()
        {
            _dispose?.Invoke(this);
            //            using (var m = _database?.Multiplexer)
            //                _database = null;
        }

        public bool IsAlive => this._GetDatabase(out var database);

        public bool GetDatabase(out IDatabase database) => _GetDatabase(out database);
        private bool _GetDatabase(out IDatabase database)
        {
            database = null;
            if (object.ReferenceEquals(this, _null_item))
                return false;
            database = _database;
            return database != null;
        }

        public T GetObject<T>(RedisKey key)
        {
            if (this._GetDatabase(out var database))
                try
                {
                    RedisValue value = database.StringGet(key);
                    if (value.HasValue)
                    {
                        try { return JsonConvert.DeserializeObject<T>(value.ToString()); }
                        catch (Exception ex) { logger.LogError(ex, "Deserialize error."); }
                    }
                }
                catch (Exception ex) { CloseConnection(ex, $"GetObjectAsync {key}"); }
            return default;
        }

        public async Task<T> GetObjectAsync<T>(RedisKey key)
        {
            if (this._GetDatabase(out var database))
                try
                {
                    RedisValue value = await database.StringGetAsync(key);
                    if (value.HasValue)
                    {
                        try { return JsonConvert.DeserializeObject<T>(value.ToString()); }
                        catch (Exception ex) { logger.LogError(ex, "Deserialize error."); }
                    }
                }
                catch (Exception ex) { CloseConnection(ex, $"GetObjectAsync {key}"); }
            return default;
        }

        public async Task<bool> SetObjectAsync<T>(RedisKey key, T obj, TimeSpan? expiry = null, When when = When.Always)
        {
            if (this._GetDatabase(out var database))
                try
                {
                    RedisValue value = JsonConvert.SerializeObject(obj);
                    return await database.StringSetAsync(key, value, expiry: expiry, when: when);
                }
                catch (Exception ex) { CloseConnection(ex, $"SetObjectAsync {key}"); }
            return default;
        }


        public List<string> GetKeys(int? db = null, string pattern = "*", int pageSize = _RedisBase.CursorUtils.DefaultLibraryPageSize, long cursor = _RedisBase.CursorUtils.Origin, int pageOffset = 0, CommandFlags flags = CommandFlags.None)
        {
            List<string> endmodel = null;
            if (this._GetDatabase(out var database))
                try
                {
                    if (GetServer(db, out var server))
                    {
                        var keys = server.Keys(db ?? database.Database, pattern, pageSize, cursor, pageOffset, flags);
                        endmodel = new List<string>();
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
            return endmodel;
        }

        public async Task<List<string>> GetKeysAsync(int? db = null, string pattern = "*") => await Task.FromResult(this.GetKeys(db, pattern));

        public System.Linq.IGrouping<string, KeyValuePair<string, string>>[] Info(RedisValue section, CommandFlags flags = CommandFlags.None)
        {
            if (GetServer(null, out var server))
                return server.Info(section, flags);
            return null;
        }

        /// <summary>
        /// 列出每個 db 的 key 數量
        /// </summary>
        /// <param name="flags"></param>
        /// <returns></returns>
        public IEnumerable<KeyValuePair<int, int>> Info_KeysCount(CommandFlags flags = CommandFlags.None)
        {
            var info = this.Info("keyspace");
            foreach (var info2 in info)
            {
                if (info2.Key == "Keyspace")
                {
                    foreach (var info3 in info2)
                    {
                        if (info3.Key.StartsWith("db", StringComparison.OrdinalIgnoreCase))
                        {
                            if (info3.Key.Substring(2).ToInt32(out int db))
                            {
                                foreach (var info4 in info3.Value.Split(','))
                                {
                                    if (info4.StartsWith("keys=", StringComparison.OrdinalIgnoreCase))
                                    {
                                        if (info4.Substring(5).ToInt32(out var n))
                                        {
                                            yield return new KeyValuePair<int, int>(db, n);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }


        public bool GetServer(int? db, out IServer server)
        {
            if (this._GetDatabase(out var database))
                if (db.HasValue)
                {
                    if (database.Database != db.Value)
                        database = database.Multiplexer.GetDatabase(db.Value);
                }

            var endPoints = database.Multiplexer.GetEndPoints();
            if (endPoints.Length > 0)
            {
                server = database.Multiplexer.GetServer(endPoints[0]);
                return true;
            }
            server = null;
            return false;
        }

        public int Database => _database.Database;

        public IConnectionMultiplexer Multiplexer => _database.Multiplexer;

        public IBatch CreateBatch(object asyncState = null)
        {
            if (this._GetDatabase(out var database))
                try { return database.CreateBatch(asyncState); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name}"); }
            return default;
        }

        public ITransaction CreateTransaction(object asyncState = null)
        {
            if (this._GetDatabase(out var database))
                try { return database.CreateTransaction(asyncState); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name}"); }
            return default;
        }

        public RedisValue DebugObject(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.DebugObject(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue> DebugObjectAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.DebugObjectAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisResult Execute(string command, params object[] args)
        {
            if (this._GetDatabase(out var database))
                try { return database.Execute(command, args); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {command}"); }
            return default;
        }

        public RedisResult Execute(string command, ICollection<object> args, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.Execute(command, args, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {command}"); }
            return default;
        }

        public async Task<RedisResult> ExecuteAsync(string command, params object[] args)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ExecuteAsync(command, args); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {command}"); }
            return default;
        }

        public async Task<RedisResult> ExecuteAsync(string command, ICollection<object> args, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ExecuteAsync(command, args, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {command}"); }
            return default;
        }

        public bool GeoAdd(RedisKey key, double longitude, double latitude, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.GeoAdd(key, longitude, latitude, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool GeoAdd(RedisKey key, GeoEntry value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.GeoAdd(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long GeoAdd(RedisKey key, GeoEntry[] values, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.GeoAdd(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> GeoAddAsync(RedisKey key, double longitude, double latitude, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.GeoAddAsync(key, longitude, latitude, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> GeoAddAsync(RedisKey key, GeoEntry value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.GeoAddAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> GeoAddAsync(RedisKey key, GeoEntry[] values, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.GeoAddAsync(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public double? GeoDistance(RedisKey key, RedisValue member1, RedisValue member2, GeoUnit unit = GeoUnit.Meters, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.GeoDistance(key, member1, member2, unit, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<double?> GeoDistanceAsync(RedisKey key, RedisValue member1, RedisValue member2, GeoUnit unit = GeoUnit.Meters, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.GeoDistanceAsync(key, member1, member2, unit, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public string[] GeoHash(RedisKey key, RedisValue[] members, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.GeoHash(key, members, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default(string[]);
        }

        public string GeoHash(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.GeoHash(key, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default(string);
        }

        public async Task<string[]> GeoHashAsync(RedisKey key, RedisValue[] members, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.GeoHashAsync(key, members, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<string> GeoHashAsync(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.GeoHashAsync(key, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public GeoPosition?[] GeoPosition(RedisKey key, RedisValue[] members, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.GeoPosition(key, members, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public GeoPosition? GeoPosition(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.GeoPosition(key, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<GeoPosition?[]> GeoPositionAsync(RedisKey key, RedisValue[] members, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.GeoPositionAsync(key, members, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<GeoPosition?> GeoPositionAsync(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.GeoPositionAsync(key, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public GeoRadiusResult[] GeoRadius(RedisKey key, RedisValue member, double radius, GeoUnit unit = GeoUnit.Meters, int count = -1, Order? order = null, GeoRadiusOptions options = GeoRadiusOptions.Default, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.GeoRadius(key, member, radius, unit, count, order, options, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public GeoRadiusResult[] GeoRadius(RedisKey key, double longitude, double latitude, double radius, GeoUnit unit = GeoUnit.Meters, int count = -1, Order? order = null, GeoRadiusOptions options = GeoRadiusOptions.Default, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.GeoRadius(key, longitude, latitude, radius, unit, count, order, options, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<GeoRadiusResult[]> GeoRadiusAsync(RedisKey key, RedisValue member, double radius, GeoUnit unit = GeoUnit.Meters, int count = -1, Order? order = null, GeoRadiusOptions options = GeoRadiusOptions.Default, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.GeoRadiusAsync(key, member, radius, unit, count, order, options, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<GeoRadiusResult[]> GeoRadiusAsync(RedisKey key, double longitude, double latitude, double radius, GeoUnit unit = GeoUnit.Meters, int count = -1, Order? order = null, GeoRadiusOptions options = GeoRadiusOptions.Default, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.GeoRadiusAsync(key, longitude, latitude, radius, unit, count, order, options, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool GeoRemove(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.GeoRemove(key, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> GeoRemoveAsync(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.GeoRemoveAsync(key, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long HashDecrement(RedisKey key, RedisValue hashField, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashDecrement(key, hashField, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public double HashDecrement(RedisKey key, RedisValue hashField, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashDecrement(key, hashField, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> HashDecrementAsync(RedisKey key, RedisValue hashField, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashDecrementAsync(key, hashField, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<double> HashDecrementAsync(RedisKey key, RedisValue hashField, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashDecrementAsync(key, hashField, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool HashDelete(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashDelete(key, hashField, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long HashDelete(RedisKey key, RedisValue[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashDelete(key, hashFields, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> HashDeleteAsync(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashDeleteAsync(key, hashField, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> HashDeleteAsync(RedisKey key, RedisValue[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashDeleteAsync(key, hashFields, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool HashExists(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashExists(key, hashField, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> HashExistsAsync(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashExistsAsync(key, hashField, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue HashGet(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashGet(key, hashField, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue[] HashGet(RedisKey key, RedisValue[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashGet(key, hashFields, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public HashEntry[] HashGetAll(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashGetAll(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<HashEntry[]> HashGetAllAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashGetAllAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue> HashGetAsync(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashGetAsync(key, hashField, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue[]> HashGetAsync(RedisKey key, RedisValue[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashGetAsync(key, hashFields, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public Lease<byte> HashGetLease(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashGetLease(key, hashField, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<Lease<byte>> HashGetLeaseAsync(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashGetLeaseAsync(key, hashField, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long HashIncrement(RedisKey key, RedisValue hashField, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashIncrement(key, hashField, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public double HashIncrement(RedisKey key, RedisValue hashField, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashIncrement(key, hashField, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> HashIncrementAsync(RedisKey key, RedisValue hashField, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashIncrementAsync(key, hashField, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<double> HashIncrementAsync(RedisKey key, RedisValue hashField, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashIncrementAsync(key, hashField, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue[] HashKeys(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashKeys(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue[]> HashKeysAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashKeysAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long HashLength(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashLength(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> HashLengthAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashLengthAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public IEnumerable<HashEntry> HashScan(RedisKey key, RedisValue pattern, int pageSize, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashScan(key, pattern, pageSize, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public IEnumerable<HashEntry> HashScan(RedisKey key, RedisValue pattern = default, int pageSize = 250, long cursor = 0, int pageOffset = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashScan(key, pattern, pageSize, cursor, pageOffset, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public IAsyncEnumerable<HashEntry> HashScanAsync(RedisKey key, RedisValue pattern = default, int pageSize = 250, long cursor = 0, int pageOffset = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashScanAsync(key, pattern, pageSize, cursor, pageOffset, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public void HashSet(RedisKey key, HashEntry[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { database.HashSet(key, hashFields, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
        }

        public bool HashSet(RedisKey key, RedisValue hashField, RedisValue value, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashSet(key, hashField, value, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task HashSetAsync(RedisKey key, HashEntry[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { await database.HashSetAsync(key, hashFields, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
        }

        public async Task<bool> HashSetAsync(RedisKey key, RedisValue hashField, RedisValue value, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashSetAsync(key, hashField, value, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long HashStringLength(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashStringLength(key, hashField, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> HashStringLengthAsync(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashStringLengthAsync(key, hashField, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue[] HashValues(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashValues(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue[]> HashValuesAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashValuesAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool HyperLogLogAdd(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HyperLogLogAdd(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool HyperLogLogAdd(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HyperLogLogAdd(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> HyperLogLogAddAsync(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HyperLogLogAddAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> HyperLogLogAddAsync(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HyperLogLogAddAsync(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long HyperLogLogLength(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HyperLogLogLength(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long HyperLogLogLength(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HyperLogLogLength(keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<long> HyperLogLogLengthAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HyperLogLogLengthAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> HyperLogLogLengthAsync(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HyperLogLogLengthAsync(keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public void HyperLogLogMerge(RedisKey destination, RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { database.HyperLogLogMerge(destination, first, second, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
        }

        public void HyperLogLogMerge(RedisKey destination, RedisKey[] sourceKeys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { database.HyperLogLogMerge(destination, sourceKeys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
        }

        public async Task HyperLogLogMergeAsync(RedisKey destination, RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { await database.HyperLogLogMergeAsync(destination, first, second, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
        }

        public async Task HyperLogLogMergeAsync(RedisKey destination, RedisKey[] sourceKeys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { await database.HyperLogLogMergeAsync(destination, sourceKeys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
        }

        public EndPoint IdentifyEndpoint(RedisKey key = default, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.IdentifyEndpoint(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<EndPoint> IdentifyEndpointAsync(RedisKey key = default, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.IdentifyEndpointAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool IsConnected(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.IsConnected(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool KeyDelete(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.KeyDelete(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long KeyDelete(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.KeyDelete(keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<bool> KeyDeleteAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.KeyDeleteAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> KeyDeleteAsync(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.KeyDeleteAsync(keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public byte[] KeyDump(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.KeyDump(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<byte[]> KeyDumpAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.KeyDumpAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool KeyExists(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.KeyExists(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long KeyExists(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.KeyExists(keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<bool> KeyExistsAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.KeyExistsAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> KeyExistsAsync(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.KeyExistsAsync(keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public bool KeyExpire(RedisKey key, TimeSpan? expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.KeyExpire(key, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool KeyExpire(RedisKey key, DateTime? expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.KeyExpire(key, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> KeyExpireAsync(RedisKey key, TimeSpan? expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.KeyExpireAsync(key, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> KeyExpireAsync(RedisKey key, DateTime? expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.KeyExpireAsync(key, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public TimeSpan? KeyIdleTime(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.KeyIdleTime(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<TimeSpan?> KeyIdleTimeAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.KeyIdleTimeAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public void KeyMigrate(RedisKey key, EndPoint toServer, int toDatabase = 0, int timeoutMilliseconds = 0, MigrateOptions migrateOptions = MigrateOptions.None, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { database.KeyMigrate(key, toServer, toDatabase, timeoutMilliseconds, migrateOptions, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
        }

        public async Task KeyMigrateAsync(RedisKey key, EndPoint toServer, int toDatabase = 0, int timeoutMilliseconds = 0, MigrateOptions migrateOptions = MigrateOptions.None, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { await database.KeyMigrateAsync(key, toServer, toDatabase, timeoutMilliseconds, migrateOptions, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
        }

        public bool KeyMove(RedisKey key, int database, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var _database))
                try { return _database.KeyMove(key, database, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> KeyMoveAsync(RedisKey key, int database, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var _database))
                try { return await _database.KeyMoveAsync(key, database, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool KeyPersist(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.KeyPersist(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> KeyPersistAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.KeyPersistAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisKey KeyRandom(CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.KeyRandom(flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<RedisKey> KeyRandomAsync(CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.KeyRandomAsync(flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public bool KeyRename(RedisKey key, RedisKey newKey, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.KeyRename(key, newKey, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> KeyRenameAsync(RedisKey key, RedisKey newKey, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.KeyRenameAsync(key, newKey, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public void KeyRestore(RedisKey key, byte[] value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { database.KeyRestore(key, value, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
        }

        public async Task KeyRestoreAsync(RedisKey key, byte[] value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { await database.KeyRestoreAsync(key, value, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
        }

        public TimeSpan? KeyTimeToLive(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.KeyTimeToLive(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<TimeSpan?> KeyTimeToLiveAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.KeyTimeToLiveAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool KeyTouch(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.KeyTouch(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long KeyTouch(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.KeyTouch(keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<bool> KeyTouchAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.KeyTouchAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> KeyTouchAsync(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.KeyTouchAsync(keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public RedisType KeyType(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.KeyType(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default(RedisType);
        }

        public async Task<RedisType> KeyTypeAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.KeyTypeAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue ListGetByIndex(RedisKey key, long index, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ListGetByIndex(key, index, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue> ListGetByIndexAsync(RedisKey key, long index, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ListGetByIndexAsync(key, index, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long ListInsertAfter(RedisKey key, RedisValue pivot, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ListInsertAfter(key, pivot, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> ListInsertAfterAsync(RedisKey key, RedisValue pivot, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ListInsertAfterAsync(key, pivot, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long ListInsertBefore(RedisKey key, RedisValue pivot, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ListInsertBefore(key, pivot, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> ListInsertBeforeAsync(RedisKey key, RedisValue pivot, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ListInsertBeforeAsync(key, pivot, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue ListLeftPop(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ListLeftPop(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue[] ListLeftPop(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ListLeftPop(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue> ListLeftPopAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ListLeftPopAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue[]> ListLeftPopAsync(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ListLeftPopAsync(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long ListLeftPush(RedisKey key, RedisValue value, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ListLeftPush(key, value, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default(long);
        }

        public long ListLeftPush(RedisKey key, RedisValue[] values, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ListLeftPush(key, values, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long ListLeftPush(RedisKey key, RedisValue[] values, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
                try { return database.ListLeftPush(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> ListLeftPushAsync(RedisKey key, RedisValue value, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ListLeftPushAsync(key, value, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> ListLeftPushAsync(RedisKey key, RedisValue[] values, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ListLeftPushAsync(key, values, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> ListLeftPushAsync(RedisKey key, RedisValue[] values, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ListLeftPushAsync(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long ListLength(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ListLength(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> ListLengthAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ListLengthAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue[] ListRange(RedisKey key, long start = 0, long stop = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ListRange(key, start, stop, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue[]> ListRangeAsync(RedisKey key, long start = 0, long stop = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ListRangeAsync(key, start, stop, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long ListRemove(RedisKey key, RedisValue value, long count = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ListRemove(key, value, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> ListRemoveAsync(RedisKey key, RedisValue value, long count = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ListRemoveAsync(key, value, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue ListRightPop(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ListRightPop(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue[] ListRightPop(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ListRightPop(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue> ListRightPopAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ListRightPopAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue[]> ListRightPopAsync(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ListRightPopAsync(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue ListRightPopLeftPush(RedisKey source, RedisKey destination, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ListRightPopLeftPush(source, destination, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<RedisValue> ListRightPopLeftPushAsync(RedisKey source, RedisKey destination, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ListRightPopLeftPushAsync(source, destination, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public long ListRightPush(RedisKey key, RedisValue value, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ListRightPush(key, value, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long ListRightPush(RedisKey key, RedisValue[] values, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ListRightPush(key, values, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long ListRightPush(RedisKey key, RedisValue[] values, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
                try { return database.ListRightPush(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> ListRightPushAsync(RedisKey key, RedisValue value, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ListRightPushAsync(key, value, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> ListRightPushAsync(RedisKey key, RedisValue[] values, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ListRightPushAsync(key, values, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> ListRightPushAsync(RedisKey key, RedisValue[] values, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ListRightPushAsync(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public void ListSetByIndex(RedisKey key, long index, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { database.ListSetByIndex(key, index, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
        }

        public async Task ListSetByIndexAsync(RedisKey key, long index, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { await database.ListSetByIndexAsync(key, index, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
        }

        public void ListTrim(RedisKey key, long start, long stop, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { database.ListTrim(key, start, stop, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
        }

        public async Task ListTrimAsync(RedisKey key, long start, long stop, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { await database.ListTrimAsync(key, start, stop, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
        }

        public bool LockExtend(RedisKey key, RedisValue value, TimeSpan expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.LockExtend(key, value, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> LockExtendAsync(RedisKey key, RedisValue value, TimeSpan expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.LockExtendAsync(key, value, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue LockQuery(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.LockQuery(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue> LockQueryAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.LockQueryAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool LockRelease(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.LockRelease(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> LockReleaseAsync(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.LockReleaseAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool LockTake(RedisKey key, RedisValue value, TimeSpan expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.LockTake(key, value, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> LockTakeAsync(RedisKey key, RedisValue value, TimeSpan expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.LockTakeAsync(key, value, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public TimeSpan Ping(CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.Ping(flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<TimeSpan> PingAsync(CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.PingAsync(flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public long Publish(RedisChannel channel, RedisValue message, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.Publish(channel, message, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {channel}"); }
            return default;
        }

        public async Task<long> PublishAsync(RedisChannel channel, RedisValue message, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.PublishAsync(channel, message, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public RedisResult ScriptEvaluate(string script, RedisKey[] keys = null, RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ScriptEvaluate(script, keys, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public RedisResult ScriptEvaluate(byte[] hash, RedisKey[] keys = null, RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ScriptEvaluate(hash, keys, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public RedisResult ScriptEvaluate(LuaScript script, object parameters = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ScriptEvaluate(script, parameters, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public RedisResult ScriptEvaluate(LoadedLuaScript script, object parameters = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ScriptEvaluate(script, parameters, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<RedisResult> ScriptEvaluateAsync(string script, RedisKey[] keys = null, RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ScriptEvaluateAsync(script, keys, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<RedisResult> ScriptEvaluateAsync(byte[] hash, RedisKey[] keys = null, RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ScriptEvaluateAsync(hash, keys, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<RedisResult> ScriptEvaluateAsync(LuaScript script, object parameters = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ScriptEvaluateAsync(script, parameters, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<RedisResult> ScriptEvaluateAsync(LoadedLuaScript script, object parameters = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ScriptEvaluateAsync(script, parameters, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public bool SetAdd(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SetAdd(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long SetAdd(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SetAdd(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> SetAddAsync(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SetAddAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> SetAddAsync(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SetAddAsync(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue[] SetCombine(SetOperation operation, RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SetCombine(operation, first, second, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public RedisValue[] SetCombine(SetOperation operation, RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SetCombine(operation, keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public long SetCombineAndStore(SetOperation operation, RedisKey destination, RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SetCombineAndStore(operation, destination, first, second, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public long SetCombineAndStore(SetOperation operation, RedisKey destination, RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SetCombineAndStore(operation, destination, keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<long> SetCombineAndStoreAsync(SetOperation operation, RedisKey destination, RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SetCombineAndStoreAsync(operation, destination, first, second, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<long> SetCombineAndStoreAsync(SetOperation operation, RedisKey destination, RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SetCombineAndStoreAsync(operation, destination, keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<RedisValue[]> SetCombineAsync(SetOperation operation, RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SetCombineAsync(operation, first, second, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<RedisValue[]> SetCombineAsync(SetOperation operation, RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SetCombineAsync(operation, keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public bool SetContains(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SetContains(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> SetContainsAsync(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SetContainsAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long SetLength(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SetLength(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> SetLengthAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SetLengthAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue[] SetMembers(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SetMembers(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue[]> SetMembersAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SetMembersAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool SetMove(RedisKey source, RedisKey destination, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SetMove(source, destination, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<bool> SetMoveAsync(RedisKey source, RedisKey destination, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SetMoveAsync(source, destination, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public RedisValue SetPop(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SetPop(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue[] SetPop(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SetPop(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue> SetPopAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SetPopAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue[]> SetPopAsync(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SetPopAsync(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue SetRandomMember(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SetRandomMember(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue> SetRandomMemberAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SetRandomMemberAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue[] SetRandomMembers(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SetRandomMembers(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue[]> SetRandomMembersAsync(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SetRandomMembersAsync(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool SetRemove(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SetRemove(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long SetRemove(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SetRemove(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> SetRemoveAsync(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SetRemoveAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> SetRemoveAsync(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SetRemoveAsync(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public IEnumerable<RedisValue> SetScan(RedisKey key, RedisValue pattern, int pageSize, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
                try { return database.SetScan(key, pattern, pageSize, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public IEnumerable<RedisValue> SetScan(RedisKey key, RedisValue pattern = default, int pageSize = 250, long cursor = 0, int pageOffset = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SetScan(key, pattern, pageSize, cursor, pageOffset, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public IAsyncEnumerable<RedisValue> SetScanAsync(RedisKey key, RedisValue pattern = default, int pageSize = 250, long cursor = 0, int pageOffset = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SetScanAsync(key, pattern, pageSize, cursor, pageOffset, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue[] Sort(RedisKey key, long skip = 0, long take = -1, Order order = Order.Ascending, SortType sortType = SortType.Numeric, RedisValue by = default, RedisValue[] get = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.Sort(key, skip, take, order, sortType, by, get, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long SortAndStore(RedisKey destination, RedisKey key, long skip = 0, long take = -1, Order order = Order.Ascending, SortType sortType = SortType.Numeric, RedisValue by = default, RedisValue[] get = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortAndStore(destination, key, skip, take, order, sortType, by, get, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> SortAndStoreAsync(RedisKey destination, RedisKey key, long skip = 0, long take = -1, Order order = Order.Ascending, SortType sortType = SortType.Numeric, RedisValue by = default, RedisValue[] get = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortAndStoreAsync(destination, key, skip, take, order, sortType, by, get, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue[]> SortAsync(RedisKey key, long skip = 0, long take = -1, Order order = Order.Ascending, SortType sortType = SortType.Numeric, RedisValue by = default, RedisValue[] get = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortAsync(key, skip, take, order, sortType, by, get, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool SortedSetAdd(RedisKey key, RedisValue member, double score, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetAdd(key, member, score, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool SortedSetAdd(RedisKey key, RedisValue member, double score, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetAdd(key, member, score, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long SortedSetAdd(RedisKey key, SortedSetEntry[] values, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetAdd(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long SortedSetAdd(RedisKey key, SortedSetEntry[] values, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetAdd(key, values, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> SortedSetAddAsync(RedisKey key, RedisValue member, double score, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetAddAsync(key, member, score, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> SortedSetAddAsync(RedisKey key, RedisValue member, double score, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetAddAsync(key, member, score, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> SortedSetAddAsync(RedisKey key, SortedSetEntry[] values, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetAddAsync(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> SortedSetAddAsync(RedisKey key, SortedSetEntry[] values, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetAddAsync(key, values, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long SortedSetCombineAndStore(SetOperation operation, RedisKey destination, RedisKey first, RedisKey second, Aggregate aggregate = Aggregate.Sum, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetCombineAndStore(operation, destination, first, second, aggregate, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public long SortedSetCombineAndStore(SetOperation operation, RedisKey destination, RedisKey[] keys, double[] weights = null, Aggregate aggregate = Aggregate.Sum, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetCombineAndStore(operation, destination, keys, weights, aggregate, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<long> SortedSetCombineAndStoreAsync(SetOperation operation, RedisKey destination, RedisKey first, RedisKey second, Aggregate aggregate = Aggregate.Sum, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetCombineAndStoreAsync(operation, destination, first, second, aggregate, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<long> SortedSetCombineAndStoreAsync(SetOperation operation, RedisKey destination, RedisKey[] keys, double[] weights = null, Aggregate aggregate = Aggregate.Sum, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetCombineAndStoreAsync(operation, destination, keys, weights, aggregate, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public double SortedSetDecrement(RedisKey key, RedisValue member, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetDecrement(key, member, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<double> SortedSetDecrementAsync(RedisKey key, RedisValue member, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetDecrementAsync(key, member, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public double SortedSetIncrement(RedisKey key, RedisValue member, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetIncrement(key, member, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<double> SortedSetIncrementAsync(RedisKey key, RedisValue member, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetIncrementAsync(key, member, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long SortedSetLength(RedisKey key, double min = double.NegativeInfinity, double max = double.PositiveInfinity, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetLength(key, min, max, exclude, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> SortedSetLengthAsync(RedisKey key, double min = double.NegativeInfinity, double max = double.PositiveInfinity, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetLengthAsync(key, min, max, exclude, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long SortedSetLengthByValue(RedisKey key, RedisValue min, RedisValue max, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetLengthByValue(key, min, max, exclude, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> SortedSetLengthByValueAsync(RedisKey key, RedisValue min, RedisValue max, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetLengthByValueAsync(key, min, max, exclude, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public SortedSetEntry? SortedSetPop(RedisKey key, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetPop(key, order, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public SortedSetEntry[] SortedSetPop(RedisKey key, long count, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetPop(key, count, order, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<SortedSetEntry?> SortedSetPopAsync(RedisKey key, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetPopAsync(key, order, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<SortedSetEntry[]> SortedSetPopAsync(RedisKey key, long count, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetPopAsync(key, count, order, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long SortedSetRangeAndStore(RedisKey sourceKey, RedisKey destinationKey, RedisValue start, RedisValue stop, SortedSetOrder sortedSetOrder = SortedSetOrder.ByRank, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long? take = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetRangeAndStore(sourceKey, destinationKey, start, stop, sortedSetOrder, exclude, order, skip, take, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<long> SortedSetRangeAndStoreAsync(RedisKey sourceKey, RedisKey destinationKey, RedisValue start, RedisValue stop, SortedSetOrder sortedSetOrder = SortedSetOrder.ByRank, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long? take = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetRangeAndStoreAsync(sourceKey, destinationKey, start, stop, sortedSetOrder, exclude, order, skip, take, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public RedisValue[] SortedSetRangeByRank(RedisKey key, long start = 0, long stop = -1, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetRangeByRank(key, start, stop, order, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue[]> SortedSetRangeByRankAsync(RedisKey key, long start = 0, long stop = -1, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetRangeByRankAsync(key, start, stop, order, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public SortedSetEntry[] SortedSetRangeByRankWithScores(RedisKey key, long start = 0, long stop = -1, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetRangeByRankWithScores(key, start, stop, order, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<SortedSetEntry[]> SortedSetRangeByRankWithScoresAsync(RedisKey key, long start = 0, long stop = -1, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetRangeByRankWithScoresAsync(key, start, stop, order, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue[] SortedSetRangeByScore(RedisKey key, double start = double.NegativeInfinity, double stop = double.PositiveInfinity, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetRangeByScore(key, start, stop, exclude, order, skip, take, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue[]> SortedSetRangeByScoreAsync(RedisKey key, double start = double.NegativeInfinity, double stop = double.PositiveInfinity, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetRangeByScoreAsync(key, start, stop, exclude, order, skip, take, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public SortedSetEntry[] SortedSetRangeByScoreWithScores(RedisKey key, double start = double.NegativeInfinity, double stop = double.PositiveInfinity, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetRangeByScoreWithScores(key, start, stop, exclude, order, skip, take, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<SortedSetEntry[]> SortedSetRangeByScoreWithScoresAsync(RedisKey key, double start = double.NegativeInfinity, double stop = double.PositiveInfinity, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetRangeByScoreWithScoresAsync(key, start, stop, exclude, order, skip, take, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue[] SortedSetRangeByValue(RedisKey key, RedisValue min, RedisValue max, Exclude exclude, long skip, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetRangeByValue(key, min, max, exclude, skip, take, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue[] SortedSetRangeByValue(RedisKey key, RedisValue min = default, RedisValue max = default, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetRangeByValue(key, min, max, exclude, order, skip, take, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue[]> SortedSetRangeByValueAsync(RedisKey key, RedisValue min, RedisValue max, Exclude exclude, long skip, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetRangeByValueAsync(key, min, max, exclude, skip, take, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue[]> SortedSetRangeByValueAsync(RedisKey key, RedisValue min = default, RedisValue max = default, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetRangeByValueAsync(key, min, max, exclude, order, skip, take, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long? SortedSetRank(RedisKey key, RedisValue member, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetRank(key, member, order, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long?> SortedSetRankAsync(RedisKey key, RedisValue member, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetRankAsync(key, member, order, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool SortedSetRemove(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetRemove(key, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long SortedSetRemove(RedisKey key, RedisValue[] members, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetRemove(key, members, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> SortedSetRemoveAsync(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetRemoveAsync(key, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> SortedSetRemoveAsync(RedisKey key, RedisValue[] members, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetRemoveAsync(key, members, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long SortedSetRemoveRangeByRank(RedisKey key, long start, long stop, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetRemoveRangeByRank(key, start, stop, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> SortedSetRemoveRangeByRankAsync(RedisKey key, long start, long stop, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetRemoveRangeByRankAsync(key, start, stop, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long SortedSetRemoveRangeByScore(RedisKey key, double start, double stop, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetRemoveRangeByScore(key, start, stop, exclude, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> SortedSetRemoveRangeByScoreAsync(RedisKey key, double start, double stop, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetRemoveRangeByScoreAsync(key, start, stop, exclude, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long SortedSetRemoveRangeByValue(RedisKey key, RedisValue min, RedisValue max, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetRemoveRangeByValue(key, min, max, exclude, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> SortedSetRemoveRangeByValueAsync(RedisKey key, RedisValue min, RedisValue max, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetRemoveRangeByValueAsync(key, min, max, exclude, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public IEnumerable<SortedSetEntry> SortedSetScan(RedisKey key, RedisValue pattern, int pageSize, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetScan(key, pattern, pageSize, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public IEnumerable<SortedSetEntry> SortedSetScan(RedisKey key, RedisValue pattern = default, int pageSize = 250, long cursor = 0, int pageOffset = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetScan(key, pattern, pageSize, cursor, pageOffset, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public IAsyncEnumerable<SortedSetEntry> SortedSetScanAsync(RedisKey key, RedisValue pattern = default, int pageSize = 250, long cursor = 0, int pageOffset = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetScanAsync(key, pattern, pageSize, cursor, pageOffset, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public double? SortedSetScore(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetScore(key, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<double?> SortedSetScoreAsync(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetScoreAsync(key, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long StreamAcknowledge(RedisKey key, RedisValue groupName, RedisValue messageId, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamAcknowledge(key, groupName, messageId, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long StreamAcknowledge(RedisKey key, RedisValue groupName, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamAcknowledge(key, groupName, messageIds, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> StreamAcknowledgeAsync(RedisKey key, RedisValue groupName, RedisValue messageId, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamAcknowledgeAsync(key, groupName, messageId, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> StreamAcknowledgeAsync(RedisKey key, RedisValue groupName, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamAcknowledgeAsync(key, groupName, messageIds, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue StreamAdd(RedisKey key, RedisValue streamField, RedisValue streamValue, RedisValue? messageId = null, int? maxLength = null, bool useApproximateMaxLength = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamAdd(key, streamField, streamValue, messageId, maxLength, useApproximateMaxLength, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue StreamAdd(RedisKey key, NameValueEntry[] streamPairs, RedisValue? messageId = null, int? maxLength = null, bool useApproximateMaxLength = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamAdd(key, streamPairs, messageId, maxLength, useApproximateMaxLength, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue> StreamAddAsync(RedisKey key, RedisValue streamField, RedisValue streamValue, RedisValue? messageId = null, int? maxLength = null, bool useApproximateMaxLength = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamAddAsync(key, streamField, streamValue, messageId, maxLength, useApproximateMaxLength, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue> StreamAddAsync(RedisKey key, NameValueEntry[] streamPairs, RedisValue? messageId = null, int? maxLength = null, bool useApproximateMaxLength = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamAddAsync(key, streamPairs, messageId, maxLength, useApproximateMaxLength, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public StreamEntry[] StreamClaim(RedisKey key, RedisValue consumerGroup, RedisValue claimingConsumer, long minIdleTimeInMs, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamClaim(key, consumerGroup, claimingConsumer, minIdleTimeInMs, messageIds, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<StreamEntry[]> StreamClaimAsync(RedisKey key, RedisValue consumerGroup, RedisValue claimingConsumer, long minIdleTimeInMs, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamClaimAsync(key, consumerGroup, claimingConsumer, minIdleTimeInMs, messageIds, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue[] StreamClaimIdsOnly(RedisKey key, RedisValue consumerGroup, RedisValue claimingConsumer, long minIdleTimeInMs, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamClaimIdsOnly(key, consumerGroup, claimingConsumer, minIdleTimeInMs, messageIds, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue[]> StreamClaimIdsOnlyAsync(RedisKey key, RedisValue consumerGroup, RedisValue claimingConsumer, long minIdleTimeInMs, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamClaimIdsOnlyAsync(key, consumerGroup, claimingConsumer, minIdleTimeInMs, messageIds, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool StreamConsumerGroupSetPosition(RedisKey key, RedisValue groupName, RedisValue position, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamConsumerGroupSetPosition(key, groupName, position, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> StreamConsumerGroupSetPositionAsync(RedisKey key, RedisValue groupName, RedisValue position, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamConsumerGroupSetPositionAsync(key, groupName, position, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public StreamConsumerInfo[] StreamConsumerInfo(RedisKey key, RedisValue groupName, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamConsumerInfo(key, groupName, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<StreamConsumerInfo[]> StreamConsumerInfoAsync(RedisKey key, RedisValue groupName, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamConsumerInfoAsync(key, groupName, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool StreamCreateConsumerGroup(RedisKey key, RedisValue groupName, RedisValue? position, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamCreateConsumerGroup(key, groupName, position, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool StreamCreateConsumerGroup(RedisKey key, RedisValue groupName, RedisValue? position = null, bool createStream = true, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamCreateConsumerGroup(key, groupName, position, createStream, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> StreamCreateConsumerGroupAsync(RedisKey key, RedisValue groupName, RedisValue? position, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamCreateConsumerGroupAsync(key, groupName, position, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> StreamCreateConsumerGroupAsync(RedisKey key, RedisValue groupName, RedisValue? position = null, bool createStream = true, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamCreateConsumerGroupAsync(key, groupName, position, createStream, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long StreamDelete(RedisKey key, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamDelete(key, messageIds, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> StreamDeleteAsync(RedisKey key, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamDeleteAsync(key, messageIds, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long StreamDeleteConsumer(RedisKey key, RedisValue groupName, RedisValue consumerName, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamDeleteConsumer(key, groupName, consumerName, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> StreamDeleteConsumerAsync(RedisKey key, RedisValue groupName, RedisValue consumerName, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamDeleteConsumerAsync(key, groupName, consumerName, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool StreamDeleteConsumerGroup(RedisKey key, RedisValue groupName, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamDeleteConsumerGroup(key, groupName, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> StreamDeleteConsumerGroupAsync(RedisKey key, RedisValue groupName, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamDeleteConsumerGroupAsync(key, groupName, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public StreamGroupInfo[] StreamGroupInfo(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamGroupInfo(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<StreamGroupInfo[]> StreamGroupInfoAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamGroupInfoAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public StreamInfo StreamInfo(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamInfo(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<StreamInfo> StreamInfoAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamInfoAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long StreamLength(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamLength(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> StreamLengthAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamLengthAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public StreamPendingInfo StreamPending(RedisKey key, RedisValue groupName, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamPending(key, groupName, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<StreamPendingInfo> StreamPendingAsync(RedisKey key, RedisValue groupName, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamPendingAsync(key, groupName, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public StreamPendingMessageInfo[] StreamPendingMessages(RedisKey key, RedisValue groupName, int count, RedisValue consumerName, RedisValue? minId = null, RedisValue? maxId = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamPendingMessages(key, groupName, count, consumerName, minId, maxId, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<StreamPendingMessageInfo[]> StreamPendingMessagesAsync(RedisKey key, RedisValue groupName, int count, RedisValue consumerName, RedisValue? minId = null, RedisValue? maxId = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamPendingMessagesAsync(key, groupName, count, consumerName, minId, maxId, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public StreamEntry[] StreamRange(RedisKey key, RedisValue? minId = null, RedisValue? maxId = null, int? count = null, Order messageOrder = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamRange(key, minId, maxId, count, messageOrder, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<StreamEntry[]> StreamRangeAsync(RedisKey key, RedisValue? minId = null, RedisValue? maxId = null, int? count = null, Order messageOrder = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamRangeAsync(key, minId, maxId, count, messageOrder, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public StreamEntry[] StreamRead(RedisKey key, RedisValue position, int? count = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamRead(key, position, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisStream[] StreamRead(StreamPosition[] streamPositions, int? countPerStream = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamRead(streamPositions, countPerStream, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<StreamEntry[]> StreamReadAsync(RedisKey key, RedisValue position, int? count = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamReadAsync(key, position, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisStream[]> StreamReadAsync(StreamPosition[] streamPositions, int? countPerStream = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamReadAsync(streamPositions, countPerStream, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public StreamEntry[] StreamReadGroup(RedisKey key, RedisValue groupName, RedisValue consumerName, RedisValue? position, int? count, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamReadGroup(key, groupName, consumerName, position, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public StreamEntry[] StreamReadGroup(RedisKey key, RedisValue groupName, RedisValue consumerName, RedisValue? position = null, int? count = null, bool noAck = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamReadGroup(key, groupName, consumerName, position, count, noAck, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisStream[] StreamReadGroup(StreamPosition[] streamPositions, RedisValue groupName, RedisValue consumerName, int? countPerStream, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamReadGroup(streamPositions, groupName, consumerName, countPerStream, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public RedisStream[] StreamReadGroup(StreamPosition[] streamPositions, RedisValue groupName, RedisValue consumerName, int? countPerStream = null, bool noAck = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamReadGroup(streamPositions, groupName, consumerName, countPerStream, noAck, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<StreamEntry[]> StreamReadGroupAsync(RedisKey key, RedisValue groupName, RedisValue consumerName, RedisValue? position, int? count, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamReadGroupAsync(key, groupName, consumerName, position, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<StreamEntry[]> StreamReadGroupAsync(RedisKey key, RedisValue groupName, RedisValue consumerName, RedisValue? position = null, int? count = null, bool noAck = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamReadGroupAsync(key, groupName, consumerName, position, count, noAck, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisStream[]> StreamReadGroupAsync(StreamPosition[] streamPositions, RedisValue groupName, RedisValue consumerName, int? countPerStream, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamReadGroupAsync(streamPositions, groupName, consumerName, countPerStream, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<RedisStream[]> StreamReadGroupAsync(StreamPosition[] streamPositions, RedisValue groupName, RedisValue consumerName, int? countPerStream = null, bool noAck = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamReadGroupAsync(streamPositions, groupName, consumerName, countPerStream, noAck, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public long StreamTrim(RedisKey key, int maxLength, bool useApproximateMaxLength = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamTrim(key, maxLength, useApproximateMaxLength, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> StreamTrimAsync(RedisKey key, int maxLength, bool useApproximateMaxLength = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamTrimAsync(key, maxLength, useApproximateMaxLength, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long StringAppend(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringAppend(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> StringAppendAsync(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringAppendAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long StringBitCount(RedisKey key, long start = 0, long end = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringBitCount(key, start, end, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> StringBitCountAsync(RedisKey key, long start = 0, long end = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringBitCountAsync(key, start, end, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long StringBitOperation(Bitwise operation, RedisKey destination, RedisKey first, RedisKey second = default, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringBitOperation(operation, destination, first, second, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public long StringBitOperation(Bitwise operation, RedisKey destination, RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringBitOperation(operation, destination, keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<long> StringBitOperationAsync(Bitwise operation, RedisKey destination, RedisKey first, RedisKey second = default, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringBitOperationAsync(operation, destination, first, second, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<long> StringBitOperationAsync(Bitwise operation, RedisKey destination, RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringBitOperationAsync(operation, destination, keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public long StringBitPosition(RedisKey key, bool bit, long start = 0, long end = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringBitPosition(key, bit, start, end, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> StringBitPositionAsync(RedisKey key, bool bit, long start = 0, long end = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringBitPositionAsync(key, bit, start, end, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long StringDecrement(RedisKey key, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringDecrement(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public double StringDecrement(RedisKey key, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringDecrement(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> StringDecrementAsync(RedisKey key, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringDecrementAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<double> StringDecrementAsync(RedisKey key, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringDecrementAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue StringGet(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringGet(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue[] StringGet(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringGet(keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public async Task<RedisValue> StringGetAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringGetAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue[]> StringGetAsync(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringGetAsync(keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public bool StringGetBit(RedisKey key, long offset, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringGetBit(key, offset, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> StringGetBitAsync(RedisKey key, long offset, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringGetBitAsync(key, offset, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue StringGetDelete(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringGetDelete(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue> StringGetDeleteAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringGetDeleteAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public Lease<byte> StringGetLease(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringGetLease(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<Lease<byte>> StringGetLeaseAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringGetLeaseAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue StringGetRange(RedisKey key, long start, long end, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringGetRange(key, start, end, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue> StringGetRangeAsync(RedisKey key, long start, long end, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringGetRangeAsync(key, start, end, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue StringGetSet(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringGetSet(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue> StringGetSetAsync(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringGetSetAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue StringGetSetExpiry(RedisKey key, TimeSpan? expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringGetSetExpiry(key, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue StringGetSetExpiry(RedisKey key, DateTime expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringGetSetExpiry(key, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue> StringGetSetExpiryAsync(RedisKey key, TimeSpan? expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringGetSetExpiryAsync(key, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue> StringGetSetExpiryAsync(RedisKey key, DateTime expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringGetSetExpiryAsync(key, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValueWithExpiry StringGetWithExpiry(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringGetWithExpiry(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValueWithExpiry> StringGetWithExpiryAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringGetWithExpiryAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long StringIncrement(RedisKey key, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringIncrement(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public double StringIncrement(RedisKey key, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringIncrement(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> StringIncrementAsync(RedisKey key, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringIncrementAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<double> StringIncrementAsync(RedisKey key, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringIncrementAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long StringLength(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringLength(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> StringLengthAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringLengthAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool StringSet(RedisKey key, RedisValue value, TimeSpan? expiry, When when, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringSet(key, value, expiry, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool StringSet(RedisKey key, RedisValue value, TimeSpan? expiry = null, bool keepTtl = false, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringSet(key, value, expiry, keepTtl, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool StringSet(KeyValuePair<RedisKey, RedisValue>[] values, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringSet(values, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public RedisValue StringSetAndGet(RedisKey key, RedisValue value, TimeSpan? expiry, When when, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringSetAndGet(key, value, expiry, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue StringSetAndGet(RedisKey key, RedisValue value, TimeSpan? expiry = null, bool keepTtl = false, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringSetAndGet(key, value, expiry, keepTtl, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue> StringSetAndGetAsync(RedisKey key, RedisValue value, TimeSpan? expiry, When when, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringSetAndGetAsync(key, value, expiry, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue> StringSetAndGetAsync(RedisKey key, RedisValue value, TimeSpan? expiry = null, bool keepTtl = false, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringSetAndGetAsync(key, value, expiry, keepTtl, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> StringSetAsync(RedisKey key, RedisValue value, TimeSpan? expiry, When when, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringSetAsync(key, value, expiry, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> StringSetAsync(RedisKey key, RedisValue value, TimeSpan? expiry = null, bool keepTtl = false, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringSetAsync(key, value, expiry, keepTtl, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> StringSetAsync(KeyValuePair<RedisKey, RedisValue>[] values, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringSetAsync(values, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public bool StringSetBit(RedisKey key, long offset, bool bit, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringSetBit(key, offset, bit, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> StringSetBitAsync(RedisKey key, long offset, bool bit, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringSetBitAsync(key, offset, bit, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue StringSetRange(RedisKey key, long offset, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringSetRange(key, offset, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue> StringSetRangeAsync(RedisKey key, long offset, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringSetRangeAsync(key, offset, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool TryWait(Task task)
        {
            if (this._GetDatabase(out var database))
                try { return database.TryWait(task); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default;
        }

        public void Wait(Task task)
        {
            if (this._GetDatabase(out var database))
                try { database.Wait(task); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
        }

        public T Wait<T>(Task<T> task)
        {
            if (this._GetDatabase(out var database))
                try { return database.Wait(task); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
            return default(T);
        }

        public void WaitAll(params Task[] tasks)
        {
            if (this._GetDatabase(out var database))
                try { database.WaitAll(tasks); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} "); }
        }

        public GeoRadiusResult[] GeoSearch(RedisKey key, RedisValue member, GeoSearchShape shape, int count = -1, bool demandClosest = true, Order? order = null, GeoRadiusOptions options = GeoRadiusOptions.Default, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.GeoSearch(key, member, shape, count, demandClosest, order, options, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public GeoRadiusResult[] GeoSearch(RedisKey key, double longitude, double latitude, GeoSearchShape shape, int count = -1, bool demandClosest = true, Order? order = null, GeoRadiusOptions options = GeoRadiusOptions.Default, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.GeoSearch(key, longitude, latitude, shape, count, demandClosest, order, options, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long GeoSearchAndStore(RedisKey sourceKey, RedisKey destinationKey, RedisValue member, GeoSearchShape shape, int count = -1, bool demandClosest = true, Order? order = null, bool storeDistances = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.GeoSearchAndStore(sourceKey, destinationKey, member, shape, count, demandClosest, order, storeDistances, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {sourceKey}"); }
            return default;
        }

        public long GeoSearchAndStore(RedisKey sourceKey, RedisKey destinationKey, double longitude, double latitude, GeoSearchShape shape, int count, bool demandClosest, Order? order, bool storeDistances, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
                try { return database.GeoSearchAndStore(sourceKey, destinationKey, longitude, latitude, shape, count, demandClosest, order, storeDistances, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {sourceKey}"); }
            return default;
        }

        public ExpireResult[] HashFieldExpire(RedisKey key, RedisValue[] hashFields, TimeSpan expiry, ExpireWhen when = ExpireWhen.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashFieldExpire(key, hashFields, expiry, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public ExpireResult[] HashFieldExpire(RedisKey key, RedisValue[] hashFields, DateTime expiry, ExpireWhen when = ExpireWhen.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashFieldExpire(key, hashFields, expiry, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long[] HashFieldGetExpireDateTime(RedisKey key, RedisValue[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashFieldGetExpireDateTime(key, hashFields, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public PersistResult[] HashFieldPersist(RedisKey key, RedisValue[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashFieldPersist(key, hashFields, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long[] HashFieldGetTimeToLive(RedisKey key, RedisValue[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashFieldGetTimeToLive(key, hashFields, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue HashRandomField(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashRandomField(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue[] HashRandomFields(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashRandomFields(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public HashEntry[] HashRandomFieldsWithValues(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashRandomFieldsWithValues(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        private static class _RedisBase
        {
            public static class CursorUtils
            {
                internal const long Origin = 0;
                internal const int
                    DefaultRedisPageSize = 10,
                    DefaultLibraryPageSize = 250;
            }
        }

        public IEnumerable<RedisValue> HashScanNoValues(RedisKey key, RedisValue pattern = default, int pageSize = _RedisBase.CursorUtils.DefaultLibraryPageSize, long cursor = _RedisBase.CursorUtils.Origin, int pageOffset = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashScanNoValues(key, pattern, pageSize, cursor, pageOffset, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool KeyCopy(RedisKey sourceKey, RedisKey destinationKey, int destinationDatabase = -1, bool replace = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.KeyCopy(sourceKey, destinationKey, destinationDatabase, replace, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {sourceKey} {destinationKey}"); }
            return default;
        }

        public string KeyEncoding(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.KeyEncoding(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool KeyExpire(RedisKey key, TimeSpan? expiry, ExpireWhen when = ExpireWhen.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.KeyExpire(key, expiry, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public bool KeyExpire(RedisKey key, DateTime? expiry, ExpireWhen when = ExpireWhen.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.KeyExpire(key, expiry, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public DateTime? KeyExpireTime(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.KeyExpireTime(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long? KeyFrequency(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.KeyFrequency(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long? KeyRefCount(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.KeyRefCount(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public ListPopResult ListLeftPop(RedisKey[] keys, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ListLeftPop(keys, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name}"); }
            return default;
        }

        public long ListPosition(RedisKey key, RedisValue element, long rank = 1, long maxLength = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ListPosition(key, element, rank, maxLength, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long[] ListPositions(RedisKey key, RedisValue element, long count, long rank = 1, long maxLength = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ListPositions(key, element, count, rank, maxLength, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue ListMove(RedisKey sourceKey, RedisKey destinationKey, ListSide sourceSide, ListSide destinationSide, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ListMove(sourceKey, destinationKey, sourceSide, destinationSide, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {sourceKey} {destinationKey}"); }
            return default;
        }

        public ListPopResult ListRightPop(RedisKey[] keys, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ListRightPop(keys, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name}"); }
            return default;
        }

        public RedisResult ScriptEvaluateReadOnly(string script, RedisKey[] keys = null, RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ScriptEvaluateReadOnly(script, keys, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name}"); }
            return default;
        }

        public RedisResult ScriptEvaluateReadOnly(byte[] hash, RedisKey[] keys = null, RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.ScriptEvaluateReadOnly(hash, keys, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name}"); }
            return default;
        }

        public bool[] SetContains(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SetContains(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long SetIntersectionLength(RedisKey[] keys, long limit = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SetIntersectionLength(keys, limit, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name}"); }
            return default;
        }

        public bool SortedSetAdd(RedisKey key, RedisValue member, double score, SortedSetWhen when = SortedSetWhen.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetAdd(key, member, score, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long SortedSetAdd(RedisKey key, SortedSetEntry[] values, SortedSetWhen when = SortedSetWhen.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetAdd(key, values, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue[] SortedSetCombine(SetOperation operation, RedisKey[] keys, double[] weights = null, Aggregate aggregate = Aggregate.Sum, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetCombine(operation, keys, weights, aggregate, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name}"); }
            return default;
        }

        public SortedSetEntry[] SortedSetCombineWithScores(SetOperation operation, RedisKey[] keys, double[] weights = null, Aggregate aggregate = Aggregate.Sum, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetCombineWithScores(operation, keys, weights, aggregate, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name}"); }
            return default;
        }

        public long SortedSetIntersectionLength(RedisKey[] keys, long limit = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetIntersectionLength(keys, limit, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name}"); }
            return default;
        }

        public RedisValue SortedSetRandomMember(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetRandomMember(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public RedisValue[] SortedSetRandomMembers(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetRandomMembers(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public SortedSetEntry[] SortedSetRandomMembersWithScores(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetRandomMembersWithScores(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public double?[] SortedSetScores(RedisKey key, RedisValue[] members, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetScores(key, members, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public SortedSetPopResult SortedSetPop(RedisKey[] keys, long count, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetPop(keys, count, order, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name}"); }
            return default;
        }

        public bool SortedSetUpdate(RedisKey key, RedisValue member, double score, SortedSetWhen when = SortedSetWhen.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetUpdate(key, member, score, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long SortedSetUpdate(RedisKey key, SortedSetEntry[] values, SortedSetWhen when = SortedSetWhen.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.SortedSetUpdate(key, values, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public StreamAutoClaimResult StreamAutoClaim(RedisKey key, RedisValue consumerGroup, RedisValue claimingConsumer, long minIdleTimeInMs, RedisValue startAtId, int? count = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamAutoClaim(key, consumerGroup, claimingConsumer, minIdleTimeInMs, startAtId, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public StreamAutoClaimIdsOnlyResult StreamAutoClaimIdsOnly(RedisKey key, RedisValue consumerGroup, RedisValue claimingConsumer, long minIdleTimeInMs, RedisValue startAtId, int? count = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StreamAutoClaimIdsOnly(key, consumerGroup, claimingConsumer, minIdleTimeInMs, startAtId, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long StringBitCount(RedisKey key, long start = 0, long end = -1, StringIndexType indexType = StringIndexType.Byte, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringBitCount(key, start, end, indexType, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public long StringBitPosition(RedisKey key, bool bit, long start = 0, long end = -1, StringIndexType indexType = StringIndexType.Byte, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringBitPosition(key, bit, start, end, indexType, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public string StringLongestCommonSubsequence(RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringLongestCommonSubsequence(first, second, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {first} {second}"); }
            return default;
        }

        public long StringLongestCommonSubsequenceLength(RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringLongestCommonSubsequenceLength(first, second, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name}"); }
            return default;
        }

        public LCSMatchResult StringLongestCommonSubsequenceWithMatches(RedisKey first, RedisKey second, long minLength = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringLongestCommonSubsequenceWithMatches(first, second, minLength, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {first} {second}"); }
            return default;
        }

        public bool StringSet(RedisKey key, RedisValue value, TimeSpan? expiry, When when)
        {
            if (this._GetDatabase(out var database))
                try { return database.StringSet(key, value, expiry, when); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<GeoRadiusResult[]> GeoSearchAsync(RedisKey key, RedisValue member, GeoSearchShape shape, int count = -1, bool demandClosest = true, Order? order = null, GeoRadiusOptions options = GeoRadiusOptions.Default, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.GeoSearchAsync(key, member, shape, count, demandClosest, order, options, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<GeoRadiusResult[]> GeoSearchAsync(RedisKey key, double longitude, double latitude, GeoSearchShape shape, int count = -1, bool demandClosest = true, Order? order = null, GeoRadiusOptions options = GeoRadiusOptions.Default, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.GeoSearchAsync(key, longitude, latitude, shape, count, demandClosest, order, options, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> GeoSearchAndStoreAsync(RedisKey sourceKey, RedisKey destinationKey, RedisValue member, GeoSearchShape shape, int count = -1, bool demandClosest = true, Order? order = null, bool storeDistances = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.GeoSearchAndStoreAsync(sourceKey, destinationKey, member, shape, count, demandClosest, order, storeDistances, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {sourceKey} {destinationKey}"); }
            return default;
        }

        public async Task<long> GeoSearchAndStoreAsync(RedisKey sourceKey, RedisKey destinationKey, double longitude, double latitude, GeoSearchShape shape, int count = -1, bool demandClosest = true, Order? order = null, bool storeDistances = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.GeoSearchAndStoreAsync(sourceKey, destinationKey, longitude, latitude, shape, count, demandClosest, order, storeDistances, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {sourceKey} {destinationKey}"); }
            return default;
        }

        public async Task<ExpireResult[]> HashFieldExpireAsync(RedisKey key, RedisValue[] hashFields, TimeSpan expiry, ExpireWhen when = ExpireWhen.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashFieldExpireAsync(key, hashFields, expiry, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<ExpireResult[]> HashFieldExpireAsync(RedisKey key, RedisValue[] hashFields, DateTime expiry, ExpireWhen when = ExpireWhen.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashFieldExpireAsync(key, hashFields, expiry, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long[]> HashFieldGetExpireDateTimeAsync(RedisKey key, RedisValue[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashFieldGetExpireDateTimeAsync(key, hashFields, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<PersistResult[]> HashFieldPersistAsync(RedisKey key, RedisValue[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashFieldPersistAsync(key, hashFields, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long[]> HashFieldGetTimeToLiveAsync(RedisKey key, RedisValue[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashFieldGetTimeToLiveAsync(key, hashFields, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue> HashRandomFieldAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashRandomFieldAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue[]> HashRandomFieldsAsync(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashRandomFieldsAsync(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<HashEntry[]> HashRandomFieldsWithValuesAsync(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.HashRandomFieldsWithValuesAsync(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public IAsyncEnumerable<RedisValue> HashScanNoValuesAsync(RedisKey key, RedisValue pattern = default, int pageSize = _RedisBase.CursorUtils.DefaultLibraryPageSize, long cursor = _RedisBase.CursorUtils.Origin, int pageOffset = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return database.HashScanNoValuesAsync(key, pattern, pageSize, cursor, pageOffset, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> KeyCopyAsync(RedisKey sourceKey, RedisKey destinationKey, int destinationDatabase = -1, bool replace = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.KeyCopyAsync(sourceKey, destinationKey, destinationDatabase, replace, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {sourceKey} {destinationKey}"); }
            return default;
        }

        public async Task<string> KeyEncodingAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.KeyEncodingAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> KeyExpireAsync(RedisKey key, TimeSpan? expiry, ExpireWhen when = ExpireWhen.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.KeyExpireAsync(key, expiry, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> KeyExpireAsync(RedisKey key, DateTime? expiry, ExpireWhen when = ExpireWhen.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.KeyExpireAsync(key, expiry, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<DateTime?> KeyExpireTimeAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.KeyExpireTimeAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long?> KeyFrequencyAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.KeyFrequencyAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long?> KeyRefCountAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.KeyRefCountAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<ListPopResult> ListLeftPopAsync(RedisKey[] keys, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ListLeftPopAsync(keys, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name}"); }
            return default;
        }

        public async Task<long> ListPositionAsync(RedisKey key, RedisValue element, long rank = 1, long maxLength = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ListPositionAsync(key, element, rank, maxLength, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long[]> ListPositionsAsync(RedisKey key, RedisValue element, long count, long rank = 1, long maxLength = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ListPositionsAsync(key, element, count, rank, maxLength, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue> ListMoveAsync(RedisKey sourceKey, RedisKey destinationKey, ListSide sourceSide, ListSide destinationSide, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ListMoveAsync(sourceKey, destinationKey, sourceSide, destinationSide, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {sourceKey} {destinationKey}"); }
            return default;
        }

        public async Task<ListPopResult> ListRightPopAsync(RedisKey[] keys, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ListRightPopAsync(keys, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name}"); }
            return default;
        }

        public async Task<RedisResult> ScriptEvaluateReadOnlyAsync(string script, RedisKey[] keys = null, RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ScriptEvaluateReadOnlyAsync(script, keys, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name}"); }
            return default;
        }

        public async Task<RedisResult> ScriptEvaluateReadOnlyAsync(byte[] hash, RedisKey[] keys = null, RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.ScriptEvaluateReadOnlyAsync(hash, keys, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name}"); }
            return default;
        }

        public async Task<bool[]> SetContainsAsync(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SetContainsAsync(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> SetIntersectionLengthAsync(RedisKey[] keys, long limit = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SetIntersectionLengthAsync(keys, limit, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name}"); }
            return default;
        }

        public async Task<bool> SortedSetAddAsync(RedisKey key, RedisValue member, double score, SortedSetWhen when = SortedSetWhen.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetAddAsync(key, member, score, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> SortedSetAddAsync(RedisKey key, SortedSetEntry[] values, SortedSetWhen when = SortedSetWhen.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetAddAsync(key, values, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue[]> SortedSetCombineAsync(SetOperation operation, RedisKey[] keys, double[] weights = null, Aggregate aggregate = Aggregate.Sum, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetCombineAsync(operation, keys, weights, aggregate, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name}"); }
            return default;
        }

        public async Task<SortedSetEntry[]> SortedSetCombineWithScoresAsync(SetOperation operation, RedisKey[] keys, double[] weights = null, Aggregate aggregate = Aggregate.Sum, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetCombineWithScoresAsync(operation, keys, weights, aggregate, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name}"); }
            return default;
        }

        public async Task<long> SortedSetIntersectionLengthAsync(RedisKey[] keys, long limit = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetIntersectionLengthAsync(keys, limit, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name}"); }
            return default;
        }

        public async Task<RedisValue> SortedSetRandomMemberAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetRandomMemberAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<RedisValue[]> SortedSetRandomMembersAsync(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetRandomMembersAsync(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<SortedSetEntry[]> SortedSetRandomMembersWithScoresAsync(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetRandomMembersWithScoresAsync(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<double?[]> SortedSetScoresAsync(RedisKey key, RedisValue[] members, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetScoresAsync(key, members, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<bool> SortedSetUpdateAsync(RedisKey key, RedisValue member, double score, SortedSetWhen when = SortedSetWhen.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetUpdateAsync(key, member, score, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> SortedSetUpdateAsync(RedisKey key, SortedSetEntry[] values, SortedSetWhen when = SortedSetWhen.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetUpdateAsync(key, values, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<SortedSetPopResult> SortedSetPopAsync(RedisKey[] keys, long count, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.SortedSetPopAsync(keys, count, order, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name}"); }
            return default;
        }

        public async Task<StreamAutoClaimResult> StreamAutoClaimAsync(RedisKey key, RedisValue consumerGroup, RedisValue claimingConsumer, long minIdleTimeInMs, RedisValue startAtId, int? count = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamAutoClaimAsync(key, consumerGroup, claimingConsumer, minIdleTimeInMs, startAtId, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<StreamAutoClaimIdsOnlyResult> StreamAutoClaimIdsOnlyAsync(RedisKey key, RedisValue consumerGroup, RedisValue claimingConsumer, long minIdleTimeInMs, RedisValue startAtId, int? count = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StreamAutoClaimIdsOnlyAsync(key, consumerGroup, claimingConsumer, minIdleTimeInMs, startAtId, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> StringBitCountAsync(RedisKey key, long start = 0, long end = -1, StringIndexType indexType = StringIndexType.Byte, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringBitCountAsync(key, start, end, indexType, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<long> StringBitPositionAsync(RedisKey key, bool bit, long start = 0, long end = -1, StringIndexType indexType = StringIndexType.Byte, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringBitPositionAsync(key, bit, start, end, indexType, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }

        public async Task<string> StringLongestCommonSubsequenceAsync(RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringLongestCommonSubsequenceAsync(first, second, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {first} {second}"); }
            return default;
        }

        public async Task<long> StringLongestCommonSubsequenceLengthAsync(RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringLongestCommonSubsequenceLengthAsync(first, second, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {first} {second}"); }
            return default;
        }

        public async Task<LCSMatchResult> StringLongestCommonSubsequenceWithMatchesAsync(RedisKey first, RedisKey second, long minLength = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringLongestCommonSubsequenceWithMatchesAsync(first, second, minLength, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {first} {second}"); }
            return default;
        }

        public async Task<bool> StringSetAsync(RedisKey key, RedisValue value, TimeSpan? expiry, When when)
        {
            if (this._GetDatabase(out var database))
                try { return await database.StringSetAsync(key, value, expiry, when); }
                catch (Exception ex) { CloseConnection(ex, $"{MethodBase.GetCurrentMethod().Name} {key}"); }
            return default;
        }
    }
}