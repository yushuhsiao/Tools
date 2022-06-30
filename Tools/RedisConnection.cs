using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace StackExchange.Redis
{
    public sealed class RedisConnection : RedisConnectionExtensions.RedisConnectionBase, IDisposable//, IDatabase
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
            _dispose(this);
        }

        void IDisposable.Dispose()
        {
            _dispose(this);
//            using (var m = _database?.Multiplexer)
//                _database = null;
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
                        try { return JsonConvert.DeserializeObject<T>(value.ToString()); }
                        catch (Exception ex) { logger.LogError(ex, "Deserialize error."); }
                    }
                }
                catch (Exception ex) { CloseConnection(ex, $"GetObjectAsync {key}"); }
            }
            return await Task.FromResult(default(T));
        }

        public async Task<bool> SetObjectAsync<T>(RedisKey key, T obj, TimeSpan? expiry = null, When when = When.Always)
        {
            if (this._GetDatabase(out var database))
            {
                RedisValue value = JsonConvert.SerializeObject(obj);
                try { return await database.StringSetAsync(key, value, expiry: expiry, when: when); }
                catch (Exception ex) { CloseConnection(ex, $"SetObjectAsync {key}"); }
            }
            return await Task.FromResult(default(bool));
        }


        public async Task<List<string>> GetKeys(int db, string pattern = "*")
        {
            List<string> endmodel = null;

            if (this._GetDatabase(out var database))
            {
                try
                {
                    if (GetServer(db, out var server))
                    {
                        var keys = server.Keys(db, pattern);
                        endmodel = new List<string>();
                        foreach (var m in keys)
                            endmodel.Add(m);
                    }
                    //if (database.Database != db)
                    //    database = database.Multiplexer.GetDatabase(db);

                    //endmodel = new List<string>();
                    //var endPoints = database.Multiplexer.GetEndPoints();
                    //if (endPoints.Length > 0)
                    //{
                    //    var server = database.Multiplexer.GetServer(endPoints[0]);

                    //    var keys = server.Keys(db, pattern);

                    //    foreach (var m in keys)
                    //        endmodel.Add(m);
                    //}
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
                                    if ( info4.StartsWith("keys=", StringComparison.OrdinalIgnoreCase))
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


        private bool GetServer(int? db, out IServer server)
        {
            if (this._GetDatabase(out var database))
            {
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
            }
            server = null;
            return false;
        }





        public int Database => _database.Database;

        public IConnectionMultiplexer Multiplexer => _database.Multiplexer;

        public IBatch CreateBatch(object asyncState = null)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.CreateBatch(asyncState); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(CreateBatch)}"); }
            }
            return default(IBatch);
        }

        public ITransaction CreateTransaction(object asyncState = null)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.CreateTransaction(asyncState); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(CreateTransaction)}"); }
            }
            return default(ITransaction);
        }

        public RedisValue DebugObject(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.DebugObject(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(DebugObject)} {key}"); }
            }
            return default(RedisValue);
        }

        public async Task<RedisValue> DebugObjectAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.DebugObjectAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(DebugObjectAsync)} {key}"); }
            }
            return await Task.FromResult(default(RedisValue));
        }

        public RedisResult Execute(string command, params object[] args)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.Execute(command, args); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(Execute)} {command}"); }
            }
            return default(RedisResult);
        }

        public RedisResult Execute(string command, ICollection<object> args, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.Execute(command, args, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(Execute)} {command}"); }
            }
            return default(RedisResult);
        }

        public async Task<RedisResult> ExecuteAsync(string command, params object[] args)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.ExecuteAsync(command, args); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ExecuteAsync)} {command}"); }
            }
            return await Task.FromResult(default(RedisResult));
        }

        public async Task<RedisResult> ExecuteAsync(string command, ICollection<object> args, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.ExecuteAsync(command, args, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ExecuteAsync)} {command}"); }
            }
            return await Task.FromResult(default(RedisResult));
        }

        public bool GeoAdd(RedisKey key, double longitude, double latitude, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.GeoAdd(key, longitude, latitude, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(GeoAdd)} {key}"); }
            }
            return default(bool);
        }

        public bool GeoAdd(RedisKey key, GeoEntry value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.GeoAdd(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(GeoAdd)} {key}"); }
            }
            return default(bool);
        }

        public long GeoAdd(RedisKey key, GeoEntry[] values, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.GeoAdd(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(GeoAdd)} {key}"); }
            }
            return default(long);
        }

        public async Task<bool> GeoAddAsync(RedisKey key, double longitude, double latitude, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.GeoAddAsync(key, longitude, latitude, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(GeoAddAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<bool> GeoAddAsync(RedisKey key, GeoEntry value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.GeoAddAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(GeoAddAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<long> GeoAddAsync(RedisKey key, GeoEntry[] values, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.GeoAddAsync(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(GeoAddAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public double? GeoDistance(RedisKey key, RedisValue member1, RedisValue member2, GeoUnit unit = GeoUnit.Meters, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.GeoDistance(key, member1, member2, unit, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(GeoDistance)} {key}"); }
            }
            return default(double?);
        }

        public async Task<double?> GeoDistanceAsync(RedisKey key, RedisValue member1, RedisValue member2, GeoUnit unit = GeoUnit.Meters, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.GeoDistanceAsync(key, member1, member2, unit, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(GeoDistanceAsync)} {key}"); }
            }
            return await Task.FromResult(default(double?));
        }

        public string[] GeoHash(RedisKey key, RedisValue[] members, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.GeoHash(key, members, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(GeoHash)} {key}"); }
            }
            return default(string[]);
        }

        public string GeoHash(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.GeoHash(key, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(GeoHash)} {key}"); }
            }
            return default(string);
        }

        public async Task<string[]> GeoHashAsync(RedisKey key, RedisValue[] members, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.GeoHashAsync(key, members, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(GeoHashAsync)} {key}"); }
            }
            return await Task.FromResult(default(string[]));
        }

        public async Task<string> GeoHashAsync(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.GeoHashAsync(key, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(GeoHashAsync)} {key}"); }
            }
            return await Task.FromResult(default(string));
        }

        public GeoPosition?[] GeoPosition(RedisKey key, RedisValue[] members, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.GeoPosition(key, members, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(GeoPosition)} {key}"); }
            }
            return default(GeoPosition?[]);
        }

        public GeoPosition? GeoPosition(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.GeoPosition(key, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(GeoPosition)} {key}"); }
            }
            return default(GeoPosition?);
        }

        public async Task<GeoPosition?[]> GeoPositionAsync(RedisKey key, RedisValue[] members, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.GeoPositionAsync(key, members, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(GeoPositionAsync)} {key}"); }
            }
            return await Task.FromResult(default(GeoPosition?[]));
        }

        public async Task<GeoPosition?> GeoPositionAsync(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.GeoPositionAsync(key, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(GeoPositionAsync)} {key}"); }
            }
            return await Task.FromResult(default(GeoPosition?));
        }

        public GeoRadiusResult[] GeoRadius(RedisKey key, RedisValue member, double radius, GeoUnit unit = GeoUnit.Meters, int count = -1, Order? order = null, GeoRadiusOptions options = GeoRadiusOptions.Default, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.GeoRadius(key, member, radius, unit, count, order, options, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(GeoRadius)} {key}"); }
            }
            return default(GeoRadiusResult[]);
        }

        public GeoRadiusResult[] GeoRadius(RedisKey key, double longitude, double latitude, double radius, GeoUnit unit = GeoUnit.Meters, int count = -1, Order? order = null, GeoRadiusOptions options = GeoRadiusOptions.Default, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.GeoRadius(key, longitude, latitude, radius, unit, count, order, options, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(GeoRadius)} {key}"); }
            }
            return default(GeoRadiusResult[]);
        }

        public async Task<GeoRadiusResult[]> GeoRadiusAsync(RedisKey key, RedisValue member, double radius, GeoUnit unit = GeoUnit.Meters, int count = -1, Order? order = null, GeoRadiusOptions options = GeoRadiusOptions.Default, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.GeoRadiusAsync(key, member, radius, unit, count, order, options, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(GeoRadiusAsync)} {key}"); }
            }
            return await Task.FromResult(default(GeoRadiusResult[]));
        }

        public async Task<GeoRadiusResult[]> GeoRadiusAsync(RedisKey key, double longitude, double latitude, double radius, GeoUnit unit = GeoUnit.Meters, int count = -1, Order? order = null, GeoRadiusOptions options = GeoRadiusOptions.Default, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.GeoRadiusAsync(key, longitude, latitude, radius, unit, count, order, options, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(GeoRadiusAsync)} {key}"); }
            }
            return await Task.FromResult(default(GeoRadiusResult[]));
        }

        public bool GeoRemove(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.GeoRemove(key, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(GeoRemove)} {key}"); }
            }
            return default(bool);
        }

        public async Task<bool> GeoRemoveAsync(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.GeoRemoveAsync(key, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(GeoRemoveAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public long HashDecrement(RedisKey key, RedisValue hashField, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.HashDecrement(key, hashField, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashDecrement)} {key}"); }
            }
            return default(long);
        }

        public double HashDecrement(RedisKey key, RedisValue hashField, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.HashDecrement(key, hashField, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashDecrement)} {key}"); }
            }
            return default(double);
        }

        public async Task<long> HashDecrementAsync(RedisKey key, RedisValue hashField, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.HashDecrementAsync(key, hashField, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashDecrementAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public async Task<double> HashDecrementAsync(RedisKey key, RedisValue hashField, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.HashDecrementAsync(key, hashField, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashDecrementAsync)} {key}"); }
            }
            return await Task.FromResult(default(double));
        }

        public bool HashDelete(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.HashDelete(key, hashField, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashDelete)} {key}"); }
            }
            return default(bool);
        }

        public long HashDelete(RedisKey key, RedisValue[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.HashDelete(key, hashFields, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashDelete)} {key}"); }
            }
            return default(long);
        }

        public async Task<bool> HashDeleteAsync(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.HashDeleteAsync(key, hashField, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashDeleteAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<long> HashDeleteAsync(RedisKey key, RedisValue[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.HashDeleteAsync(key, hashFields, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashDeleteAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public bool HashExists(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.HashExists(key, hashField, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashExists)} {key}"); }
            }
            return default(bool);
        }

        public async Task<bool> HashExistsAsync(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.HashExistsAsync(key, hashField, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashExistsAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public RedisValue HashGet(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.HashGet(key, hashField, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashGet)} {key}"); }
            }
            return default(RedisValue);
        }

        public RedisValue[] HashGet(RedisKey key, RedisValue[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.HashGet(key, hashFields, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashGet)} {key}"); }
            }
            return default(RedisValue[]);
        }

        public HashEntry[] HashGetAll(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.HashGetAll(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashGetAll)} {key}"); }
            }
            return default(HashEntry[]);
        }

        public async Task<HashEntry[]> HashGetAllAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.HashGetAllAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashGetAllAsync)} {key}"); }
            }
            return await Task.FromResult(default(HashEntry[]));
        }

        public async Task<RedisValue> HashGetAsync(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.HashGetAsync(key, hashField, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashGetAsync)} {key}"); }
            }
            return await Task.FromResult(default(RedisValue));
        }

        public async Task<RedisValue[]> HashGetAsync(RedisKey key, RedisValue[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.HashGetAsync(key, hashFields, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashGetAsync)} {key}"); }
            }
            return await Task.FromResult(default(RedisValue[]));
        }

        public Lease<byte> HashGetLease(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.HashGetLease(key, hashField, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashGetLease)} {key}"); }
            }
            return default(Lease<byte>);
        }

        public async Task<Lease<byte>> HashGetLeaseAsync(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.HashGetLeaseAsync(key, hashField, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashGetLeaseAsync)} {key}"); }
            }
            return await Task.FromResult(default(Lease<byte>));
        }

        public long HashIncrement(RedisKey key, RedisValue hashField, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.HashIncrement(key, hashField, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashIncrement)} {key}"); }
            }
            return default(long);
        }

        public double HashIncrement(RedisKey key, RedisValue hashField, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.HashIncrement(key, hashField, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashIncrement)} {key}"); }
            }
            return default(double);
        }

        public async Task<long> HashIncrementAsync(RedisKey key, RedisValue hashField, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.HashIncrementAsync(key, hashField, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashIncrementAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public async Task<double> HashIncrementAsync(RedisKey key, RedisValue hashField, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.HashIncrementAsync(key, hashField, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashIncrementAsync)} {key}"); }
            }
            return await Task.FromResult(default(double));
        }

        public RedisValue[] HashKeys(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.HashKeys(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashKeys)} {key}"); }
            }
            return default(RedisValue[]);
        }

        public async Task<RedisValue[]> HashKeysAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.HashKeysAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashKeysAsync)} {key}"); }
            }
            return await Task.FromResult(default(RedisValue[]));
        }

        public long HashLength(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.HashLength(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashLength)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> HashLengthAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.HashLengthAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashLengthAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public IEnumerable<HashEntry> HashScan(RedisKey key, RedisValue pattern, int pageSize, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.HashScan(key, pattern, pageSize, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashScan)} {key}"); }
            }
            return default(IEnumerable<HashEntry>);
        }

        public IEnumerable<HashEntry> HashScan(RedisKey key, RedisValue pattern = default, int pageSize = 250, long cursor = 0, int pageOffset = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.HashScan(key, pattern, pageSize, cursor, pageOffset, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashScan)} {key}"); }
            }
            return default(IEnumerable<HashEntry>);
        }

        public IAsyncEnumerable<HashEntry> HashScanAsync(RedisKey key, RedisValue pattern = default, int pageSize = 250, long cursor = 0, int pageOffset = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.HashScanAsync(key, pattern, pageSize, cursor, pageOffset, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashScanAsync)} {key}"); }
            }
            return default(IAsyncEnumerable<HashEntry>);
        }

        public void HashSet(RedisKey key, HashEntry[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { database.HashSet(key, hashFields, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashSet)} {key}"); }
            }
        }

        public bool HashSet(RedisKey key, RedisValue hashField, RedisValue value, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.HashSet(key, hashField, value, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashSet)} {key}"); }
            }
            return default(bool);
        }

        public async Task HashSetAsync(RedisKey key, HashEntry[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { await database.HashSetAsync(key, hashFields, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashSetAsync)} {key}"); }
            }
        }

        public async Task<bool> HashSetAsync(RedisKey key, RedisValue hashField, RedisValue value, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.HashSetAsync(key, hashField, value, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashSetAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public long HashStringLength(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.HashStringLength(key, hashField, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashStringLength)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> HashStringLengthAsync(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.HashStringLengthAsync(key, hashField, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashStringLengthAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public RedisValue[] HashValues(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.HashValues(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashValues)} {key}"); }
            }
            return default(RedisValue[]);
        }

        public async Task<RedisValue[]> HashValuesAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.HashValuesAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HashValuesAsync)} {key}"); }
            }
            return await Task.FromResult(default(RedisValue[]));
        }

        public bool HyperLogLogAdd(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.HyperLogLogAdd(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HyperLogLogAdd)} {key}"); }
            }
            return default(bool);
        }

        public bool HyperLogLogAdd(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.HyperLogLogAdd(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HyperLogLogAdd)} {key}"); }
            }
            return default(bool);
        }

        public async Task<bool> HyperLogLogAddAsync(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.HyperLogLogAddAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HyperLogLogAddAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<bool> HyperLogLogAddAsync(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.HyperLogLogAddAsync(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HyperLogLogAddAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public long HyperLogLogLength(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.HyperLogLogLength(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HyperLogLogLength)} {key}"); }
            }
            return default(long);
        }

        public long HyperLogLogLength(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.HyperLogLogLength(keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HyperLogLogLength)} "); }
            }
            return default(long);
        }

        public async Task<long> HyperLogLogLengthAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.HyperLogLogLengthAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HyperLogLogLengthAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public async Task<long> HyperLogLogLengthAsync(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.HyperLogLogLengthAsync(keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HyperLogLogLengthAsync)} "); }
            }
            return await Task.FromResult(default(long));
        }

        public void HyperLogLogMerge(RedisKey destination, RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { database.HyperLogLogMerge(destination, first, second, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HyperLogLogMerge)} "); }
            }
        }

        public void HyperLogLogMerge(RedisKey destination, RedisKey[] sourceKeys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { database.HyperLogLogMerge(destination, sourceKeys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HyperLogLogMerge)} "); }
            }
        }

        public async Task HyperLogLogMergeAsync(RedisKey destination, RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { await database.HyperLogLogMergeAsync(destination, first, second, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HyperLogLogMergeAsync)} "); }
            }
        }

        public async Task HyperLogLogMergeAsync(RedisKey destination, RedisKey[] sourceKeys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { await database.HyperLogLogMergeAsync(destination, sourceKeys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(HyperLogLogMergeAsync)} "); }
            }
        }

        public EndPoint IdentifyEndpoint(RedisKey key = default, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.IdentifyEndpoint(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(IdentifyEndpoint)} {key}"); }
            }
            return default(EndPoint);
        }

        public async Task<EndPoint> IdentifyEndpointAsync(RedisKey key = default, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.IdentifyEndpointAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(IdentifyEndpointAsync)} {key}"); }
            }
            return default(EndPoint);
        }

        public bool IsConnected(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.IsConnected(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(IsConnected)} {key}"); }
            }
            return default(bool);
        }

        public bool KeyDelete(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.KeyDelete(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyDelete)} {key}"); }
            }
            return default(bool);
        }

        public long KeyDelete(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.KeyDelete(keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyDelete)} "); }
            }
            return default(long);
        }

        public async Task<bool> KeyDeleteAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.KeyDeleteAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyDeleteAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<long> KeyDeleteAsync(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.KeyDeleteAsync(keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyDeleteAsync)} "); }
            }
            return await Task.FromResult(default(long));
        }

        public byte[] KeyDump(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.KeyDump(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyDump)} {key}"); }
            }
            return default(byte[]);
        }

        public async Task<byte[]> KeyDumpAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.KeyDumpAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyDumpAsync)} {key}"); }
            }
            return await Task.FromResult(default(byte[]));
        }

        public bool KeyExists(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.KeyExists(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyExists)} {key}"); }
            }
            return default(bool);
        }

        public long KeyExists(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.KeyExists(keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyExists)} "); }
            }
            return default(long);
        }

        public async Task<bool> KeyExistsAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.KeyExistsAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyExistsAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<long> KeyExistsAsync(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.KeyExistsAsync(keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyExistsAsync)} "); }
            }
            return await Task.FromResult(default(long));
        }

        public bool KeyExpire(RedisKey key, TimeSpan? expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.KeyExpire(key, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyExpire)} {key}"); }
            }
            return default(bool);
        }

        public bool KeyExpire(RedisKey key, DateTime? expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.KeyExpire(key, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyExpire)} {key}"); }
            }
            return default(bool);
        }

        public async Task<bool> KeyExpireAsync(RedisKey key, TimeSpan? expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.KeyExpireAsync(key, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyExpireAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<bool> KeyExpireAsync(RedisKey key, DateTime? expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.KeyExpireAsync(key, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyExpireAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public TimeSpan? KeyIdleTime(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.KeyIdleTime(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyIdleTime)} {key}"); }
            }
            return default(TimeSpan?);
        }

        public async Task<TimeSpan?> KeyIdleTimeAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.KeyIdleTimeAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyIdleTimeAsync)} {key}"); }
            }
            return await Task.FromResult(default(TimeSpan?));
        }

        public void KeyMigrate(RedisKey key, EndPoint toServer, int toDatabase = 0, int timeoutMilliseconds = 0, MigrateOptions migrateOptions = MigrateOptions.None, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { database.KeyMigrate(key, toServer, toDatabase, timeoutMilliseconds, migrateOptions, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyMigrate)} {key}"); }
            }
        }

        public async Task KeyMigrateAsync(RedisKey key, EndPoint toServer, int toDatabase = 0, int timeoutMilliseconds = 0, MigrateOptions migrateOptions = MigrateOptions.None, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { await database.KeyMigrateAsync(key, toServer, toDatabase, timeoutMilliseconds, migrateOptions, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyMigrateAsync)} {key}"); }
            }
        }

        public bool KeyMove(RedisKey key, int database, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var _database))
            {
                try { return _database.KeyMove(key, database, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyMove)} {key}"); }
            }
            return default(bool);
        }

        public async Task<bool> KeyMoveAsync(RedisKey key, int database, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var _database))
            {
                try { return await _database.KeyMoveAsync(key, database, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyMoveAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public bool KeyPersist(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.KeyPersist(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyPersist)} {key}"); }
            }
            return default(bool);
        }

        public async Task<bool> KeyPersistAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.KeyPersistAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyPersistAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public RedisKey KeyRandom(CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.KeyRandom(flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyRandom)} "); }
            }
            return default(RedisKey);
        }

        public async Task<RedisKey> KeyRandomAsync(CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.KeyRandomAsync(flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyRandomAsync)} "); }
            }
            return await Task.FromResult(default(RedisKey));
        }

        public bool KeyRename(RedisKey key, RedisKey newKey, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.KeyRename(key, newKey, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyRename)} {key}"); }
            }
            return default(bool);
        }

        public async Task<bool> KeyRenameAsync(RedisKey key, RedisKey newKey, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.KeyRenameAsync(key, newKey, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyRenameAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public void KeyRestore(RedisKey key, byte[] value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { database.KeyRestore(key, value, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyRestore)} {key}"); }
            }
        }

        public async Task KeyRestoreAsync(RedisKey key, byte[] value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { await database.KeyRestoreAsync(key, value, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyRestoreAsync)} {key}"); }
            }
        }

        public TimeSpan? KeyTimeToLive(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.KeyTimeToLive(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyTimeToLive)} {key}"); }
            }
            return default(TimeSpan?);
        }

        public async Task<TimeSpan?> KeyTimeToLiveAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.KeyTimeToLiveAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyTimeToLiveAsync)} {key}"); }
            }
            return await Task.FromResult(default(TimeSpan?));
        }

        public bool KeyTouch(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.KeyTouch(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyTouch)} {key}"); }
            }
            return default(bool);
        }

        public long KeyTouch(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.KeyTouch(keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyTouch)} "); }
            }
            return default(long);
        }

        public async Task<bool> KeyTouchAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.KeyTouchAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyTouchAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<long> KeyTouchAsync(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.KeyTouchAsync(keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyTouchAsync)} "); }
            }
            return await Task.FromResult(default(long));
        }

        public RedisType KeyType(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.KeyType(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyType)} {key}"); }
            }
            return default(RedisType);
        }

        public async Task<RedisType> KeyTypeAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.KeyTypeAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(KeyTypeAsync)} {key}"); }
            }
            return await Task.FromResult(default(RedisType));
        }

        public RedisValue ListGetByIndex(RedisKey key, long index, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.ListGetByIndex(key, index, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListGetByIndex)} {key}"); }
            }
            return default(bool);
        }

        public async Task<RedisValue> ListGetByIndexAsync(RedisKey key, long index, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.ListGetByIndexAsync(key, index, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListGetByIndexAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public long ListInsertAfter(RedisKey key, RedisValue pivot, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.ListInsertAfter(key, pivot, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListInsertAfter)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> ListInsertAfterAsync(RedisKey key, RedisValue pivot, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.ListInsertAfterAsync(key, pivot, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListInsertAfterAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public long ListInsertBefore(RedisKey key, RedisValue pivot, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.ListInsertBefore(key, pivot, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListInsertBefore)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> ListInsertBeforeAsync(RedisKey key, RedisValue pivot, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.ListInsertBeforeAsync(key, pivot, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListInsertBeforeAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public RedisValue ListLeftPop(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.ListLeftPop(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListLeftPop)} {key}"); }
            }
            return default(bool);
        }

        public RedisValue[] ListLeftPop(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.ListLeftPop(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListLeftPop)} {key}"); }
            }
            return default(RedisValue[]);
        }

        public async Task<RedisValue> ListLeftPopAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.ListLeftPopAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListLeftPopAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<RedisValue[]> ListLeftPopAsync(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.ListLeftPopAsync(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListLeftPopAsync)} {key}"); }
            }
            return await Task.FromResult(default(RedisValue[]));
        }

        public long ListLeftPush(RedisKey key, RedisValue value, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.ListLeftPush(key, value, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListLeftPush)} {key}"); }
            }
            return default(long);
        }

        public long ListLeftPush(RedisKey key, RedisValue[] values, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.ListLeftPush(key, values, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListLeftPush)} {key}"); }
            }
            return default(long);
        }

        public long ListLeftPush(RedisKey key, RedisValue[] values, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.ListLeftPush(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListLeftPush)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> ListLeftPushAsync(RedisKey key, RedisValue value, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.ListLeftPushAsync(key, value, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListLeftPushAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public async Task<long> ListLeftPushAsync(RedisKey key, RedisValue[] values, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.ListLeftPushAsync(key, values, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListLeftPushAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public async Task<long> ListLeftPushAsync(RedisKey key, RedisValue[] values, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.ListLeftPushAsync(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListLeftPushAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public long ListLength(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.ListLength(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListLength)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> ListLengthAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.ListLengthAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListLengthAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public RedisValue[] ListRange(RedisKey key, long start = 0, long stop = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.ListRange(key, start, stop, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListRange)} {key}"); }
            }
            return default(RedisValue[]);
        }

        public async Task<RedisValue[]> ListRangeAsync(RedisKey key, long start = 0, long stop = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.ListRangeAsync(key, start, stop, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListRangeAsync)} {key}"); }
            }
            return await Task.FromResult(default(RedisValue[]));
        }

        public long ListRemove(RedisKey key, RedisValue value, long count = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.ListRemove(key, value, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListRemove)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> ListRemoveAsync(RedisKey key, RedisValue value, long count = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.ListRemoveAsync(key, value, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListRemoveAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public RedisValue ListRightPop(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.ListRightPop(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListRightPop)} {key}"); }
            }
            return default(bool);
        }

        public RedisValue[] ListRightPop(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.ListRightPop(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListRightPop)} {key}"); }
            }
            return default(RedisValue[]);
        }

        public async Task<RedisValue> ListRightPopAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.ListRightPopAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListRightPopAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<RedisValue[]> ListRightPopAsync(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.ListRightPopAsync(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListRightPopAsync)} {key}"); }
            }
            return await Task.FromResult(default(RedisValue[]));
        }

        public RedisValue ListRightPopLeftPush(RedisKey source, RedisKey destination, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.ListRightPopLeftPush(source, destination, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListRightPopLeftPush)} "); }
            }
            return default(bool);
        }

        public async Task<RedisValue> ListRightPopLeftPushAsync(RedisKey source, RedisKey destination, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.ListRightPopLeftPushAsync(source, destination, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListRightPopLeftPushAsync)} "); }
            }
            return await Task.FromResult(default(bool));
        }

        public long ListRightPush(RedisKey key, RedisValue value, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.ListRightPush(key, value, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListRightPush)} {key}"); }
            }
            return default(long);
        }

        public long ListRightPush(RedisKey key, RedisValue[] values, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.ListRightPush(key, values, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListRightPush)} {key}"); }
            }
            return default(long);
        }

        public long ListRightPush(RedisKey key, RedisValue[] values, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.ListRightPush(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListRightPush)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> ListRightPushAsync(RedisKey key, RedisValue value, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.ListRightPushAsync(key, value, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListRightPushAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public async Task<long> ListRightPushAsync(RedisKey key, RedisValue[] values, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.ListRightPushAsync(key, values, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListRightPushAsync)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> ListRightPushAsync(RedisKey key, RedisValue[] values, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.ListRightPushAsync(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListRightPushAsync)} {key}"); }
            }
            return default(long);
        }

        public void ListSetByIndex(RedisKey key, long index, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { database.ListSetByIndex(key, index, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListSetByIndex)} {key}"); }
            }
        }

        public async Task ListSetByIndexAsync(RedisKey key, long index, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { await database.ListSetByIndexAsync(key, index, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListSetByIndexAsync)} {key}"); }
            }
        }

        public void ListTrim(RedisKey key, long start, long stop, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { database.ListTrim(key, start, stop, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListTrim)} {key}"); }
            }
        }

        public async Task ListTrimAsync(RedisKey key, long start, long stop, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { await database.ListTrimAsync(key, start, stop, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ListTrimAsync)} {key}"); }
            }
        }

        public bool LockExtend(RedisKey key, RedisValue value, TimeSpan expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.LockExtend(key, value, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(LockExtend)} {key}"); }
            }
            return default(bool);
        }

        public async Task<bool> LockExtendAsync(RedisKey key, RedisValue value, TimeSpan expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.LockExtendAsync(key, value, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(LockExtendAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public RedisValue LockQuery(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.LockQuery(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(LockQuery)} {key}"); }
            }
            return default(bool);
        }

        public async Task<RedisValue> LockQueryAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.LockQueryAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(LockQueryAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public bool LockRelease(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.LockRelease(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(LockRelease)} {key}"); }
            }
            return default(bool);
        }

        public async Task<bool> LockReleaseAsync(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.LockReleaseAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(LockReleaseAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public bool LockTake(RedisKey key, RedisValue value, TimeSpan expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.LockTake(key, value, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(LockTake)} {key}"); }
            }
            return default(bool);
        }

        public async Task<bool> LockTakeAsync(RedisKey key, RedisValue value, TimeSpan expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.LockTakeAsync(key, value, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(LockTakeAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public TimeSpan Ping(CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.Ping(flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(Ping)} "); }
            }
            return default(TimeSpan);
        }

        public async Task<TimeSpan> PingAsync(CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.PingAsync(flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(PingAsync)} "); }
            }
            return await Task.FromResult(default(TimeSpan));
        }

        public long Publish(RedisChannel channel, RedisValue message, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.Publish(channel, message, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(Publish)} {channel}"); }
            }
            return default(long);
        }

        public async Task<long> PublishAsync(RedisChannel channel, RedisValue message, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.PublishAsync(channel, message, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(PublishAsync)} "); }
            }
            return await Task.FromResult(default(long));
        }

        public RedisResult ScriptEvaluate(string script, RedisKey[] keys = null, RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.ScriptEvaluate(script, keys, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ScriptEvaluate)} "); }
            }
            return default(RedisResult);
        }

        public RedisResult ScriptEvaluate(byte[] hash, RedisKey[] keys = null, RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.ScriptEvaluate(hash, keys, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ScriptEvaluate)} "); }
            }
            return default(RedisResult);
        }

        public RedisResult ScriptEvaluate(LuaScript script, object parameters = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.ScriptEvaluate(script, parameters, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ScriptEvaluate)} "); }
            }
            return default(RedisResult);
        }

        public RedisResult ScriptEvaluate(LoadedLuaScript script, object parameters = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.ScriptEvaluate(script, parameters, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ScriptEvaluate)} "); }
            }
            return default(RedisResult);
        }

        public async Task<RedisResult> ScriptEvaluateAsync(string script, RedisKey[] keys = null, RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.ScriptEvaluateAsync(script, keys, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ScriptEvaluateAsync)} "); }
            }
            return await Task.FromResult(default(RedisResult));
        }

        public async Task<RedisResult> ScriptEvaluateAsync(byte[] hash, RedisKey[] keys = null, RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.ScriptEvaluateAsync(hash, keys, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ScriptEvaluateAsync)} "); }
            }
            return await Task.FromResult(default(RedisResult));
        }

        public async Task<RedisResult> ScriptEvaluateAsync(LuaScript script, object parameters = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.ScriptEvaluateAsync(script, parameters, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ScriptEvaluateAsync)} "); }
            }
            return await Task.FromResult(default(RedisResult));
        }

        public async Task<RedisResult> ScriptEvaluateAsync(LoadedLuaScript script, object parameters = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.ScriptEvaluateAsync(script, parameters, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(ScriptEvaluateAsync)} "); }
            }
            return await Task.FromResult(default(RedisResult));
        }

        public bool SetAdd(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SetAdd(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetAdd)} {key}"); }
            }
            return default(bool);
        }

        public long SetAdd(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SetAdd(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetAdd)} {key}"); }
            }
            return default(long);
        }

        public async Task<bool> SetAddAsync(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SetAddAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetAddAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<long> SetAddAsync(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SetAddAsync(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetAddAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public RedisValue[] SetCombine(SetOperation operation, RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SetCombine(operation, first, second, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetCombine)} "); }
            }
            return default(RedisValue[]);
        }

        public RedisValue[] SetCombine(SetOperation operation, RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SetCombine(operation, keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetCombine)} "); }
            }
            return default(RedisValue[]);
        }

        public long SetCombineAndStore(SetOperation operation, RedisKey destination, RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SetCombineAndStore(operation, destination, first, second, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetCombineAndStore)} "); }
            }
            return default(long);
        }

        public long SetCombineAndStore(SetOperation operation, RedisKey destination, RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SetCombineAndStore(operation, destination, keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetCombineAndStore)} "); }
            }
            return default(long);
        }

        public async Task<long> SetCombineAndStoreAsync(SetOperation operation, RedisKey destination, RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SetCombineAndStoreAsync(operation, destination, first, second, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetCombineAndStoreAsync)} "); }
            }
            return await Task.FromResult(default(long));
        }

        public async Task<long> SetCombineAndStoreAsync(SetOperation operation, RedisKey destination, RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SetCombineAndStoreAsync(operation, destination, keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetCombineAndStoreAsync)} "); }
            }
            return await Task.FromResult(default(long));
        }

        public async Task<RedisValue[]> SetCombineAsync(SetOperation operation, RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SetCombineAsync(operation, first, second, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetCombineAsync)} "); }
            }
            return await Task.FromResult(default(RedisValue[]));
        }

        public async Task<RedisValue[]> SetCombineAsync(SetOperation operation, RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SetCombineAsync(operation, keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetCombineAsync)} "); }
            }
            return await Task.FromResult(default(RedisValue[]));
        }

        public bool SetContains(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SetContains(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetContains)} {key}"); }
            }
            return default(bool);
        }

        public async Task<bool> SetContainsAsync(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SetContainsAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetContainsAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public long SetLength(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SetLength(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetLength)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> SetLengthAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SetLengthAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetLengthAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public RedisValue[] SetMembers(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SetMembers(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetMembers)} {key}"); }
            }
            return default(RedisValue[]);
        }

        public async Task<RedisValue[]> SetMembersAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SetMembersAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetMembersAsync)} {key}"); }
            }
            return await Task.FromResult(default(RedisValue[]));
        }

        public bool SetMove(RedisKey source, RedisKey destination, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SetMove(source, destination, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetMove)} "); }
            }
            return default(bool);
        }

        public async Task<bool> SetMoveAsync(RedisKey source, RedisKey destination, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SetMoveAsync(source, destination, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetMoveAsync)} "); }
            }
            return await Task.FromResult(default(bool));
        }

        public RedisValue SetPop(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SetPop(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetPop)} {key}"); }
            }
            return default(bool);
        }

        public RedisValue[] SetPop(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SetPop(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetPop)} {key}"); }
            }
            return default(RedisValue[]);
        }

        public async Task<RedisValue> SetPopAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SetPopAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetPopAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<RedisValue[]> SetPopAsync(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SetPopAsync(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetPopAsync)} {key}"); }
            }
            return await Task.FromResult(default(RedisValue[]));
        }

        public RedisValue SetRandomMember(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SetRandomMember(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetRandomMember)} {key}"); }
            }
            return default(RedisValue);
        }

        public async Task<RedisValue> SetRandomMemberAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SetRandomMemberAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetRandomMemberAsync)} {key}"); }
            }
            return await Task.FromResult(default(RedisValue));
        }

        public RedisValue[] SetRandomMembers(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SetRandomMembers(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetRandomMembers)} {key}"); }
            }
            return default(RedisValue[]);
        }

        public async Task<RedisValue[]> SetRandomMembersAsync(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SetRandomMembersAsync(key, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetRandomMembersAsync)} {key}"); }
            }
            return await Task.FromResult(default(RedisValue[]));
        }

        public bool SetRemove(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SetRemove(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetRemove)} {key}"); }
            }
            return default(bool);
        }

        public long SetRemove(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SetRemove(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetRemove)} {key}"); }
            }
            return default(long);
        }

        public async Task<bool> SetRemoveAsync(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SetRemoveAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetRemoveAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<long> SetRemoveAsync(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SetRemoveAsync(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetRemoveAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public IEnumerable<RedisValue> SetScan(RedisKey key, RedisValue pattern, int pageSize, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SetScan(key, pattern, pageSize, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetScan)} {key}"); }
            }
            return default(IEnumerable<RedisValue>);
        }

        public IEnumerable<RedisValue> SetScan(RedisKey key, RedisValue pattern = default, int pageSize = 250, long cursor = 0, int pageOffset = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SetScan(key, pattern, pageSize, cursor, pageOffset, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetScan)} {key}"); }
            }
            return default(IEnumerable<RedisValue>);
        }

        public IAsyncEnumerable<RedisValue> SetScanAsync(RedisKey key, RedisValue pattern = default, int pageSize = 250, long cursor = 0, int pageOffset = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SetScanAsync(key, pattern, pageSize, cursor, pageOffset, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SetScanAsync)} {key}"); }
            }
            return default(IAsyncEnumerable<RedisValue>);
        }

        public RedisValue[] Sort(RedisKey key, long skip = 0, long take = -1, Order order = Order.Ascending, SortType sortType = SortType.Numeric, RedisValue by = default, RedisValue[] get = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.Sort(key, skip, take, order, sortType, by, get, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(Sort)} {key}"); }
            }
            return default(RedisValue[]);
        }

        public long SortAndStore(RedisKey destination, RedisKey key, long skip = 0, long take = -1, Order order = Order.Ascending, SortType sortType = SortType.Numeric, RedisValue by = default, RedisValue[] get = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortAndStore(destination, key, skip, take, order, sortType, by, get, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortAndStore)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> SortAndStoreAsync(RedisKey destination, RedisKey key, long skip = 0, long take = -1, Order order = Order.Ascending, SortType sortType = SortType.Numeric, RedisValue by = default, RedisValue[] get = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortAndStoreAsync(destination, key, skip, take, order, sortType, by, get, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortAndStoreAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public async Task<RedisValue[]> SortAsync(RedisKey key, long skip = 0, long take = -1, Order order = Order.Ascending, SortType sortType = SortType.Numeric, RedisValue by = default, RedisValue[] get = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortAsync(key, skip, take, order, sortType, by, get, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortAsync)} {key}"); }
            }
            return await Task.FromResult(default(RedisValue[]));
        }

        public bool SortedSetAdd(RedisKey key, RedisValue member, double score, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetAdd(key, member, score, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetAdd)} {key}"); }
            }
            return default(bool);
        }

        public bool SortedSetAdd(RedisKey key, RedisValue member, double score, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetAdd(key, member, score, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetAdd)} {key}"); }
            }
            return default(bool);
        }

        public long SortedSetAdd(RedisKey key, SortedSetEntry[] values, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetAdd(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetAdd)} {key}"); }
            }
            return default(long);
        }

        public long SortedSetAdd(RedisKey key, SortedSetEntry[] values, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetAdd(key, values, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetAdd)} {key}"); }
            }
            return default(long);
        }

        public async Task<bool> SortedSetAddAsync(RedisKey key, RedisValue member, double score, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetAddAsync(key, member, score, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetAddAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<bool> SortedSetAddAsync(RedisKey key, RedisValue member, double score, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetAddAsync(key, member, score, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetAddAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<long> SortedSetAddAsync(RedisKey key, SortedSetEntry[] values, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetAddAsync(key, values, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetAddAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public async Task<long> SortedSetAddAsync(RedisKey key, SortedSetEntry[] values, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetAddAsync(key, values, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetAddAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public long SortedSetCombineAndStore(SetOperation operation, RedisKey destination, RedisKey first, RedisKey second, Aggregate aggregate = Aggregate.Sum, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetCombineAndStore(operation, destination, first, second, aggregate, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetCombineAndStore)} "); }
            }
            return default(long);
        }

        public long SortedSetCombineAndStore(SetOperation operation, RedisKey destination, RedisKey[] keys, double[] weights = null, Aggregate aggregate = Aggregate.Sum, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetCombineAndStore(operation, destination, keys, weights, aggregate, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetCombineAndStore)} "); }
            }
            return default(long);
        }

        public async Task<long> SortedSetCombineAndStoreAsync(SetOperation operation, RedisKey destination, RedisKey first, RedisKey second, Aggregate aggregate = Aggregate.Sum, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetCombineAndStoreAsync(operation, destination, first, second, aggregate, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetCombineAndStoreAsync)} "); }
            }
            return await Task.FromResult(default(long));
        }

        public async Task<long> SortedSetCombineAndStoreAsync(SetOperation operation, RedisKey destination, RedisKey[] keys, double[] weights = null, Aggregate aggregate = Aggregate.Sum, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetCombineAndStoreAsync(operation, destination, keys, weights, aggregate, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetCombineAndStoreAsync)} "); }
            }
            return await Task.FromResult(default(long));
        }

        public double SortedSetDecrement(RedisKey key, RedisValue member, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetDecrement(key, member, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetDecrement)} {key}"); }
            }
            return default(double);
        }

        public async Task<double> SortedSetDecrementAsync(RedisKey key, RedisValue member, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetDecrementAsync(key, member, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetDecrementAsync)} {key}"); }
            }
            return await Task.FromResult(default(double));
        }

        public double SortedSetIncrement(RedisKey key, RedisValue member, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetIncrement(key, member, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetIncrement)} {key}"); }
            }
            return default(double);
        }

        public async Task<double> SortedSetIncrementAsync(RedisKey key, RedisValue member, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetIncrementAsync(key, member, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetIncrementAsync)} {key}"); }
            }
            return await Task.FromResult(default(double));
        }

        public long SortedSetLength(RedisKey key, double min = double.NegativeInfinity, double max = double.PositiveInfinity, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetLength(key, min, max, exclude, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetLength)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> SortedSetLengthAsync(RedisKey key, double min = double.NegativeInfinity, double max = double.PositiveInfinity, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetLengthAsync(key, min, max, exclude, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetLengthAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public long SortedSetLengthByValue(RedisKey key, RedisValue min, RedisValue max, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetLengthByValue(key, min, max, exclude, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetLengthByValue)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> SortedSetLengthByValueAsync(RedisKey key, RedisValue min, RedisValue max, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetLengthByValueAsync(key, min, max, exclude, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetLengthByValueAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public SortedSetEntry? SortedSetPop(RedisKey key, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetPop(key, order, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetPop)} {key}"); }
            }
            return default(SortedSetEntry?);
        }

        public SortedSetEntry[] SortedSetPop(RedisKey key, long count, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetPop(key, count, order, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetPop)} {key}"); }
            }
            return default(SortedSetEntry[]);
        }

        public async Task<SortedSetEntry?> SortedSetPopAsync(RedisKey key, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetPopAsync(key, order, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetPopAsync)} {key}"); }
            }
            return await Task.FromResult(default(SortedSetEntry?));
        }

        public async Task<SortedSetEntry[]> SortedSetPopAsync(RedisKey key, long count, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetPopAsync(key, count, order, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetPopAsync)} {key}"); }
            }
            return await Task.FromResult(default(SortedSetEntry[]));
        }

        public long SortedSetRangeAndStore(RedisKey sourceKey, RedisKey destinationKey, RedisValue start, RedisValue stop, SortedSetOrder sortedSetOrder = SortedSetOrder.ByRank, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long? take = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetRangeAndStore(sourceKey, destinationKey, start, stop, sortedSetOrder, exclude, order, skip, take, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRangeAndStore)} "); }
            }
            return default(long);
        }

        public async Task<long> SortedSetRangeAndStoreAsync(RedisKey sourceKey, RedisKey destinationKey, RedisValue start, RedisValue stop, SortedSetOrder sortedSetOrder = SortedSetOrder.ByRank, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long? take = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetRangeAndStoreAsync(sourceKey, destinationKey, start, stop, sortedSetOrder, exclude, order, skip, take, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRangeAndStoreAsync)} "); }
            }
            return await Task.FromResult(default(long));
        }

        public RedisValue[] SortedSetRangeByRank(RedisKey key, long start = 0, long stop = -1, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetRangeByRank(key, start, stop, order, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRangeByRank)} {key}"); }
            }
            return default(RedisValue[]);
        }

        public async Task<RedisValue[]> SortedSetRangeByRankAsync(RedisKey key, long start = 0, long stop = -1, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetRangeByRankAsync(key, start, stop, order, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRangeByRankAsync)} {key}"); }
            }
            return await Task.FromResult(default(RedisValue[]));
        }

        public SortedSetEntry[] SortedSetRangeByRankWithScores(RedisKey key, long start = 0, long stop = -1, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetRangeByRankWithScores(key, start, stop, order, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRangeByRankWithScores)} {key}"); }
            }
            return default(SortedSetEntry[]);
        }

        public async Task<SortedSetEntry[]> SortedSetRangeByRankWithScoresAsync(RedisKey key, long start = 0, long stop = -1, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetRangeByRankWithScoresAsync(key, start, stop, order, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRangeByRankWithScoresAsync)} {key}"); }
            }
            return await Task.FromResult(default(SortedSetEntry[]));
        }

        public RedisValue[] SortedSetRangeByScore(RedisKey key, double start = double.NegativeInfinity, double stop = double.PositiveInfinity, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetRangeByScore(key, start, stop, exclude, order, skip, take, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRangeByScore)} {key}"); }
            }
            return default(RedisValue[]);
        }

        public async Task<RedisValue[]> SortedSetRangeByScoreAsync(RedisKey key, double start = double.NegativeInfinity, double stop = double.PositiveInfinity, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetRangeByScoreAsync(key, start, stop, exclude, order, skip, take, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRangeByScoreAsync)} {key}"); }
            }
            return await Task.FromResult(default(RedisValue[]));
        }

        public SortedSetEntry[] SortedSetRangeByScoreWithScores(RedisKey key, double start = double.NegativeInfinity, double stop = double.PositiveInfinity, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetRangeByScoreWithScores(key, start, stop, exclude, order, skip, take, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRangeByScoreWithScores)} {key}"); }
            }
            return default(SortedSetEntry[]);
        }

        public async Task<SortedSetEntry[]> SortedSetRangeByScoreWithScoresAsync(RedisKey key, double start = double.NegativeInfinity, double stop = double.PositiveInfinity, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetRangeByScoreWithScoresAsync(key, start, stop, exclude, order, skip, take, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRangeByScoreWithScoresAsync)} {key}"); }
            }
            return await Task.FromResult(default(SortedSetEntry[]));
        }

        public RedisValue[] SortedSetRangeByValue(RedisKey key, RedisValue min, RedisValue max, Exclude exclude, long skip, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetRangeByValue(key, min, max, exclude, skip, take, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRangeByValue)} {key}"); }
            }
            return default(RedisValue[]);
        }

        public RedisValue[] SortedSetRangeByValue(RedisKey key, RedisValue min = default, RedisValue max = default, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetRangeByValue(key, min, max, exclude, order, skip, take, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRangeByValue)} {key}"); }
            }
            return default(RedisValue[]);
        }

        public async Task<RedisValue[]> SortedSetRangeByValueAsync(RedisKey key, RedisValue min, RedisValue max, Exclude exclude, long skip, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetRangeByValueAsync(key, min, max, exclude, skip, take, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRangeByValueAsync)} {key}"); }
            }
            return await Task.FromResult(default(RedisValue[]));
        }

        public async Task<RedisValue[]> SortedSetRangeByValueAsync(RedisKey key, RedisValue min = default, RedisValue max = default, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetRangeByValueAsync(key, min, max, exclude, order, skip, take, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRangeByValueAsync)} {key}"); }
            }
            return await Task.FromResult(default(RedisValue[]));
        }

        public long? SortedSetRank(RedisKey key, RedisValue member, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetRank(key, member, order, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRank)} {key}"); }
            }
            return default(long);
        }

        public async Task<long?> SortedSetRankAsync(RedisKey key, RedisValue member, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetRankAsync(key, member, order, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRankAsync)} {key}"); }
            }
            return await Task.FromResult(default(long?));
        }

        public bool SortedSetRemove(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetRemove(key, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRemove)} {key}"); }
            }
            return default(bool);
        }

        public long SortedSetRemove(RedisKey key, RedisValue[] members, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetRemove(key, members, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRemove)} {key}"); }
            }
            return default(long);
        }

        public async Task<bool> SortedSetRemoveAsync(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetRemoveAsync(key, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRemoveAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<long> SortedSetRemoveAsync(RedisKey key, RedisValue[] members, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetRemoveAsync(key, members, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRemoveAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public long SortedSetRemoveRangeByRank(RedisKey key, long start, long stop, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetRemoveRangeByRank(key, start, stop, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRemoveRangeByRank)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> SortedSetRemoveRangeByRankAsync(RedisKey key, long start, long stop, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetRemoveRangeByRankAsync(key, start, stop, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRemoveRangeByRankAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public long SortedSetRemoveRangeByScore(RedisKey key, double start, double stop, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetRemoveRangeByScore(key, start, stop, exclude, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRemoveRangeByScore)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> SortedSetRemoveRangeByScoreAsync(RedisKey key, double start, double stop, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetRemoveRangeByScoreAsync(key, start, stop, exclude, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRemoveRangeByScoreAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public long SortedSetRemoveRangeByValue(RedisKey key, RedisValue min, RedisValue max, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetRemoveRangeByValue(key, min, max, exclude, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRemoveRangeByValue)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> SortedSetRemoveRangeByValueAsync(RedisKey key, RedisValue min, RedisValue max, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetRemoveRangeByValueAsync(key, min, max, exclude, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetRemoveRangeByValueAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public IEnumerable<SortedSetEntry> SortedSetScan(RedisKey key, RedisValue pattern, int pageSize, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetScan(key, pattern, pageSize, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetScan)} {key}"); }
            }
            return default(IEnumerable<SortedSetEntry>);
        }

        public IEnumerable<SortedSetEntry> SortedSetScan(RedisKey key, RedisValue pattern = default, int pageSize = 250, long cursor = 0, int pageOffset = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetScan(key, pattern, pageSize, cursor, pageOffset, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetScan)} {key}"); }
            }
            return default(IEnumerable<SortedSetEntry>);
        }

        public IAsyncEnumerable<SortedSetEntry> SortedSetScanAsync(RedisKey key, RedisValue pattern = default, int pageSize = 250, long cursor = 0, int pageOffset = 0, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetScanAsync(key, pattern, pageSize, cursor, pageOffset, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetScanAsync)} {key}"); }
            }
            return default(IAsyncEnumerable<SortedSetEntry>);
        }

        public double? SortedSetScore(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.SortedSetScore(key, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetScore)} {key}"); }
            }
            return default(double?);
        }

        public async Task<double?> SortedSetScoreAsync(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.SortedSetScoreAsync(key, member, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(SortedSetScoreAsync)} {key}"); }
            }
            return await Task.FromResult(default(double?));
        }

        public long StreamAcknowledge(RedisKey key, RedisValue groupName, RedisValue messageId, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamAcknowledge(key, groupName, messageId, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamAcknowledge)} {key}"); }
            }
            return default(long);
        }

        public long StreamAcknowledge(RedisKey key, RedisValue groupName, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamAcknowledge(key, groupName, messageIds, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamAcknowledge)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> StreamAcknowledgeAsync(RedisKey key, RedisValue groupName, RedisValue messageId, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamAcknowledgeAsync(key, groupName, messageId, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamAcknowledgeAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public async Task<long> StreamAcknowledgeAsync(RedisKey key, RedisValue groupName, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamAcknowledgeAsync(key, groupName, messageIds, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamAcknowledgeAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public RedisValue StreamAdd(RedisKey key, RedisValue streamField, RedisValue streamValue, RedisValue? messageId = null, int? maxLength = null, bool useApproximateMaxLength = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamAdd(key, streamField, streamValue, messageId, maxLength, useApproximateMaxLength, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamAdd)} {key}"); }
            }
            return default(bool);
        }

        public RedisValue StreamAdd(RedisKey key, NameValueEntry[] streamPairs, RedisValue? messageId = null, int? maxLength = null, bool useApproximateMaxLength = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamAdd(key, streamPairs, messageId, maxLength, useApproximateMaxLength, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamAdd)} {key}"); }
            }
            return default(bool);
        }

        public async Task<RedisValue> StreamAddAsync(RedisKey key, RedisValue streamField, RedisValue streamValue, RedisValue? messageId = null, int? maxLength = null, bool useApproximateMaxLength = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamAddAsync(key, streamField, streamValue, messageId, maxLength, useApproximateMaxLength, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamAddAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<RedisValue> StreamAddAsync(RedisKey key, NameValueEntry[] streamPairs, RedisValue? messageId = null, int? maxLength = null, bool useApproximateMaxLength = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamAddAsync(key, streamPairs, messageId, maxLength, useApproximateMaxLength, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamAddAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public StreamEntry[] StreamClaim(RedisKey key, RedisValue consumerGroup, RedisValue claimingConsumer, long minIdleTimeInMs, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamClaim(key, consumerGroup, claimingConsumer, minIdleTimeInMs, messageIds, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamClaim)} {key}"); }
            }
            return default(StreamEntry[]);
        }

        public async Task<StreamEntry[]> StreamClaimAsync(RedisKey key, RedisValue consumerGroup, RedisValue claimingConsumer, long minIdleTimeInMs, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamClaimAsync(key, consumerGroup, claimingConsumer, minIdleTimeInMs, messageIds, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamClaimAsync)} {key}"); }
            }
            return await Task.FromResult(default(StreamEntry[]));
        }

        public RedisValue[] StreamClaimIdsOnly(RedisKey key, RedisValue consumerGroup, RedisValue claimingConsumer, long minIdleTimeInMs, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamClaimIdsOnly(key, consumerGroup, claimingConsumer, minIdleTimeInMs, messageIds, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamClaimIdsOnly)} {key}"); }
            }
            return default(RedisValue[]);
        }

        public async Task<RedisValue[]> StreamClaimIdsOnlyAsync(RedisKey key, RedisValue consumerGroup, RedisValue claimingConsumer, long minIdleTimeInMs, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamClaimIdsOnlyAsync(key, consumerGroup, claimingConsumer, minIdleTimeInMs, messageIds, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamClaimIdsOnlyAsync)} {key}"); }
            }
            return await Task.FromResult(default(RedisValue[]));
        }

        public bool StreamConsumerGroupSetPosition(RedisKey key, RedisValue groupName, RedisValue position, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamConsumerGroupSetPosition(key, groupName, position, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamConsumerGroupSetPosition)} {key}"); }
            }
            return default(bool);
        }

        public async Task<bool> StreamConsumerGroupSetPositionAsync(RedisKey key, RedisValue groupName, RedisValue position, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamConsumerGroupSetPositionAsync(key, groupName, position, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamConsumerGroupSetPositionAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public StreamConsumerInfo[] StreamConsumerInfo(RedisKey key, RedisValue groupName, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamConsumerInfo(key, groupName, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamConsumerInfo)} {key}"); }
            }
            return default(StreamConsumerInfo[]);
        }

        public async Task<StreamConsumerInfo[]> StreamConsumerInfoAsync(RedisKey key, RedisValue groupName, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamConsumerInfoAsync(key, groupName, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamConsumerInfoAsync)} {key}"); }
            }
            return await Task.FromResult(default(StreamConsumerInfo[]));
        }

        public bool StreamCreateConsumerGroup(RedisKey key, RedisValue groupName, RedisValue? position, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamCreateConsumerGroup(key, groupName, position, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamCreateConsumerGroup)} {key}"); }
            }
            return default(bool);
        }

        public bool StreamCreateConsumerGroup(RedisKey key, RedisValue groupName, RedisValue? position = null, bool createStream = true, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamCreateConsumerGroup(key, groupName, position, createStream, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamCreateConsumerGroup)} {key}"); }
            }
            return default(bool);
        }

        public async Task<bool> StreamCreateConsumerGroupAsync(RedisKey key, RedisValue groupName, RedisValue? position, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamCreateConsumerGroupAsync(key, groupName, position, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamCreateConsumerGroupAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<bool> StreamCreateConsumerGroupAsync(RedisKey key, RedisValue groupName, RedisValue? position = null, bool createStream = true, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamCreateConsumerGroupAsync(key, groupName, position, createStream, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamCreateConsumerGroupAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public long StreamDelete(RedisKey key, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamDelete(key, messageIds, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamDelete)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> StreamDeleteAsync(RedisKey key, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamDeleteAsync(key, messageIds, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamDeleteAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public long StreamDeleteConsumer(RedisKey key, RedisValue groupName, RedisValue consumerName, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamDeleteConsumer(key, groupName, consumerName, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamDeleteConsumer)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> StreamDeleteConsumerAsync(RedisKey key, RedisValue groupName, RedisValue consumerName, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamDeleteConsumerAsync(key, groupName, consumerName, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamDeleteConsumerAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public bool StreamDeleteConsumerGroup(RedisKey key, RedisValue groupName, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamDeleteConsumerGroup(key, groupName, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamDeleteConsumerGroup)} {key}"); }
            }
            return default(bool);
        }

        public async Task<bool> StreamDeleteConsumerGroupAsync(RedisKey key, RedisValue groupName, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamDeleteConsumerGroupAsync(key, groupName, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamDeleteConsumerGroupAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public StreamGroupInfo[] StreamGroupInfo(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamGroupInfo(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamGroupInfo)} {key}"); }
            }
            return default(StreamGroupInfo[]);
        }

        public async Task<StreamGroupInfo[]> StreamGroupInfoAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamGroupInfoAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamGroupInfoAsync)} {key}"); }
            }
            return await Task.FromResult(default(StreamGroupInfo[]));
        }

        public StreamInfo StreamInfo(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamInfo(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamInfo)} {key}"); }
            }
            return default(StreamInfo);
        }

        public async Task<StreamInfo> StreamInfoAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamInfoAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamInfoAsync)} {key}"); }
            }
            return await Task.FromResult(default(StreamInfo));
        }

        public long StreamLength(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamLength(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamLength)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> StreamLengthAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamLengthAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamLengthAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public StreamPendingInfo StreamPending(RedisKey key, RedisValue groupName, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamPending(key, groupName, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamPending)} {key}"); }
            }
            return default(StreamPendingInfo);
        }

        public async Task<StreamPendingInfo> StreamPendingAsync(RedisKey key, RedisValue groupName, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamPendingAsync(key, groupName, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamPendingAsync)} {key}"); }
            }
            return await Task.FromResult(default(StreamPendingInfo));
        }

        public StreamPendingMessageInfo[] StreamPendingMessages(RedisKey key, RedisValue groupName, int count, RedisValue consumerName, RedisValue? minId = null, RedisValue? maxId = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamPendingMessages(key, groupName, count, consumerName, minId, maxId, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamPendingMessages)} {key}"); }
            }
            return default(StreamPendingMessageInfo[]);
        }

        public async Task<StreamPendingMessageInfo[]> StreamPendingMessagesAsync(RedisKey key, RedisValue groupName, int count, RedisValue consumerName, RedisValue? minId = null, RedisValue? maxId = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamPendingMessagesAsync(key, groupName, count, consumerName, minId, maxId, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamPendingMessagesAsync)} {key}"); }
            }
            return await Task.FromResult(default(StreamPendingMessageInfo[]));
        }

        public StreamEntry[] StreamRange(RedisKey key, RedisValue? minId = null, RedisValue? maxId = null, int? count = null, Order messageOrder = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamRange(key, minId, maxId, count, messageOrder, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamRange)} {key}"); }
            }
            return default(StreamEntry[]);
        }

        public async Task<StreamEntry[]> StreamRangeAsync(RedisKey key, RedisValue? minId = null, RedisValue? maxId = null, int? count = null, Order messageOrder = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamRangeAsync(key, minId, maxId, count, messageOrder, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamRangeAsync)} {key}"); }
            }
            return await Task.FromResult(default(StreamEntry[]));
        }

        public StreamEntry[] StreamRead(RedisKey key, RedisValue position, int? count = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamRead(key, position, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamRead)} {key}"); }
            }
            return default(StreamEntry[]);
        }

        public RedisStream[] StreamRead(StreamPosition[] streamPositions, int? countPerStream = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamRead(streamPositions, countPerStream, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamRead)} "); }
            }
            return default(RedisStream[]);
        }

        public async Task<StreamEntry[]> StreamReadAsync(RedisKey key, RedisValue position, int? count = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamReadAsync(key, position, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamReadAsync)} {key}"); }
            }
            return await Task.FromResult(default(StreamEntry[]));
        }

        public async Task<RedisStream[]> StreamReadAsync(StreamPosition[] streamPositions, int? countPerStream = null, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamReadAsync(streamPositions, countPerStream, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamReadAsync)} "); }
            }
            return await Task.FromResult(default(RedisStream[]));
        }

        public StreamEntry[] StreamReadGroup(RedisKey key, RedisValue groupName, RedisValue consumerName, RedisValue? position, int? count, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamReadGroup(key, groupName, consumerName, position, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamReadGroup)} {key}"); }
            }
            return default(StreamEntry[]);
        }

        public StreamEntry[] StreamReadGroup(RedisKey key, RedisValue groupName, RedisValue consumerName, RedisValue? position = null, int? count = null, bool noAck = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamReadGroup(key, groupName, consumerName, position, count, noAck, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamReadGroup)} {key}"); }
            }
            return default(StreamEntry[]);
        }

        public RedisStream[] StreamReadGroup(StreamPosition[] streamPositions, RedisValue groupName, RedisValue consumerName, int? countPerStream, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamReadGroup(streamPositions, groupName, consumerName, countPerStream, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamReadGroup)} "); }
            }
            return default(RedisStream[]);
        }

        public RedisStream[] StreamReadGroup(StreamPosition[] streamPositions, RedisValue groupName, RedisValue consumerName, int? countPerStream = null, bool noAck = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamReadGroup(streamPositions, groupName, consumerName, countPerStream, noAck, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamReadGroup)} "); }
            }
            return default(RedisStream[]);
        }

        public async Task<StreamEntry[]> StreamReadGroupAsync(RedisKey key, RedisValue groupName, RedisValue consumerName, RedisValue? position, int? count, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamReadGroupAsync(key, groupName, consumerName, position, count, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamReadGroupAsync)} {key}"); }
            }
            return await Task.FromResult(default(StreamEntry[]));
        }

        public async Task<StreamEntry[]> StreamReadGroupAsync(RedisKey key, RedisValue groupName, RedisValue consumerName, RedisValue? position = null, int? count = null, bool noAck = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamReadGroupAsync(key, groupName, consumerName, position, count, noAck, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamReadGroupAsync)} {key}"); }
            }
            return await Task.FromResult(default(StreamEntry[]));
        }

        public async Task<RedisStream[]> StreamReadGroupAsync(StreamPosition[] streamPositions, RedisValue groupName, RedisValue consumerName, int? countPerStream, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamReadGroupAsync(streamPositions, groupName, consumerName, countPerStream, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamReadGroupAsync)} "); }
            }
            return await Task.FromResult(default(RedisStream[]));
        }

        public async Task<RedisStream[]> StreamReadGroupAsync(StreamPosition[] streamPositions, RedisValue groupName, RedisValue consumerName, int? countPerStream = null, bool noAck = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamReadGroupAsync(streamPositions, groupName, consumerName, countPerStream, noAck, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamReadGroupAsync)} "); }
            }
            return await Task.FromResult(default(RedisStream[]));
        }

        public long StreamTrim(RedisKey key, int maxLength, bool useApproximateMaxLength = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StreamTrim(key, maxLength, useApproximateMaxLength, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamTrim)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> StreamTrimAsync(RedisKey key, int maxLength, bool useApproximateMaxLength = false, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StreamTrimAsync(key, maxLength, useApproximateMaxLength, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StreamTrimAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public long StringAppend(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringAppend(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringAppend)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> StringAppendAsync(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringAppendAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringAppendAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public long StringBitCount(RedisKey key, long start = 0, long end = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringBitCount(key, start, end, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringBitCount)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> StringBitCountAsync(RedisKey key, long start = 0, long end = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringBitCountAsync(key, start, end, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringBitCountAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public long StringBitOperation(Bitwise operation, RedisKey destination, RedisKey first, RedisKey second = default, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringBitOperation(operation, destination, first, second, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringBitOperation)} "); }
            }
            return default(long);
        }

        public long StringBitOperation(Bitwise operation, RedisKey destination, RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringBitOperation(operation, destination, keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringBitOperation)} "); }
            }
            return default(long);
        }

        public async Task<long> StringBitOperationAsync(Bitwise operation, RedisKey destination, RedisKey first, RedisKey second = default, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringBitOperationAsync(operation, destination, first, second, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringBitOperationAsync)} "); }
            }
            return await Task.FromResult(default(long));
        }

        public async Task<long> StringBitOperationAsync(Bitwise operation, RedisKey destination, RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringBitOperationAsync(operation, destination, keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringBitOperationAsync)} "); }
            }
            return await Task.FromResult(default(long));
        }

        public long StringBitPosition(RedisKey key, bool bit, long start = 0, long end = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringBitPosition(key, bit, start, end, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringBitPosition)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> StringBitPositionAsync(RedisKey key, bool bit, long start = 0, long end = -1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringBitPositionAsync(key, bit, start, end, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringBitPositionAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public long StringDecrement(RedisKey key, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringDecrement(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringDecrement)} {key}"); }
            }
            return default(long);
        }

        public double StringDecrement(RedisKey key, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringDecrement(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringDecrement)} {key}"); }
            }
            return default(double);
        }

        public async Task<long> StringDecrementAsync(RedisKey key, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringDecrementAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringDecrementAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public async Task<double> StringDecrementAsync(RedisKey key, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringDecrementAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringDecrementAsync)} {key}"); }
            }
            return await Task.FromResult(default(double));
        }

        public RedisValue StringGet(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringGet(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringGet)} {key}"); }
            }
            return default(bool);
        }

        public RedisValue[] StringGet(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringGet(keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringGet)} "); }
            }
            return default(RedisValue[]);
        }

        public async Task<RedisValue> StringGetAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringGetAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringGetAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<RedisValue[]> StringGetAsync(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringGetAsync(keys, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringGetAsync)} "); }
            }
            return await Task.FromResult(default(RedisValue[]));
        }

        public bool StringGetBit(RedisKey key, long offset, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringGetBit(key, offset, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringGetBit)} {key}"); }
            }
            return default(bool);
        }

        public async Task<bool> StringGetBitAsync(RedisKey key, long offset, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringGetBitAsync(key, offset, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringGetBitAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public RedisValue StringGetDelete(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringGetDelete(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringGetDelete)} {key}"); }
            }
            return default(bool);
        }

        public async Task<RedisValue> StringGetDeleteAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringGetDeleteAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringGetDeleteAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public Lease<byte> StringGetLease(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringGetLease(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringGetLease)} {key}"); }
            }
            return default(Lease<byte>);
        }

        public async Task<Lease<byte>> StringGetLeaseAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringGetLeaseAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringGetLeaseAsync)} {key}"); }
            }
            return await Task.FromResult(default(Lease<byte>));
        }

        public RedisValue StringGetRange(RedisKey key, long start, long end, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringGetRange(key, start, end, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringGetRange)} {key}"); }
            }
            return default(bool);
        }

        public async Task<RedisValue> StringGetRangeAsync(RedisKey key, long start, long end, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringGetRangeAsync(key, start, end, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringGetRangeAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public RedisValue StringGetSet(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringGetSet(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringGetSet)} {key}"); }
            }
            return default(bool);
        }

        public async Task<RedisValue> StringGetSetAsync(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringGetSetAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringGetSetAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public RedisValue StringGetSetExpiry(RedisKey key, TimeSpan? expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringGetSetExpiry(key, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringGetSetExpiry)} {key}"); }
            }
            return default(bool);
        }

        public RedisValue StringGetSetExpiry(RedisKey key, DateTime expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringGetSetExpiry(key, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringGetSetExpiry)} {key}"); }
            }
            return default(bool);
        }

        public async Task<RedisValue> StringGetSetExpiryAsync(RedisKey key, TimeSpan? expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringGetSetExpiryAsync(key, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringGetSetExpiryAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<RedisValue> StringGetSetExpiryAsync(RedisKey key, DateTime expiry, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringGetSetExpiryAsync(key, expiry, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringGetSetExpiryAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public RedisValueWithExpiry StringGetWithExpiry(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringGetWithExpiry(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringGetWithExpiry)} {key}"); }
            }
            return default(RedisValueWithExpiry);
        }

        public async Task<RedisValueWithExpiry> StringGetWithExpiryAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringGetWithExpiryAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringGetWithExpiryAsync)} {key}"); }
            }
            return await Task.FromResult(default(RedisValueWithExpiry));
        }

        public long StringIncrement(RedisKey key, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringIncrement(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringIncrement)} {key}"); }
            }
            return default(long);
        }

        public double StringIncrement(RedisKey key, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringIncrement(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringIncrement)} {key}"); }
            }
            return default(double);
        }

        public async Task<long> StringIncrementAsync(RedisKey key, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringIncrementAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringIncrementAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public async Task<double> StringIncrementAsync(RedisKey key, double value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringIncrementAsync(key, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringIncrementAsync)} {key}"); }
            }
            return await Task.FromResult(default(double));
        }

        public long StringLength(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringLength(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringLength)} {key}"); }
            }
            return default(long);
        }

        public async Task<long> StringLengthAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringLengthAsync(key, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringLengthAsync)} {key}"); }
            }
            return await Task.FromResult(default(long));
        }

        public bool StringSet(RedisKey key, RedisValue value, TimeSpan? expiry, When when, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringSet(key, value, expiry, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringSet)} {key}"); }
            }
            return default(bool);
        }

        public bool StringSet(RedisKey key, RedisValue value, TimeSpan? expiry = null, bool keepTtl = false, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringSet(key, value, expiry, keepTtl, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringSet)} {key}"); }
            }
            return default(bool);
        }

        public bool StringSet(KeyValuePair<RedisKey, RedisValue>[] values, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringSet(values, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringSet)} "); }
            }
            return default(bool);
        }

        public RedisValue StringSetAndGet(RedisKey key, RedisValue value, TimeSpan? expiry, When when, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringSetAndGet(key, value, expiry, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringSetAndGet)} {key}"); }
            }
            return default(bool);
        }

        public RedisValue StringSetAndGet(RedisKey key, RedisValue value, TimeSpan? expiry = null, bool keepTtl = false, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringSetAndGet(key, value, expiry, keepTtl, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringSetAndGet)} {key}"); }
            }
            return default(bool);
        }

        public async Task<RedisValue> StringSetAndGetAsync(RedisKey key, RedisValue value, TimeSpan? expiry, When when, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringSetAndGetAsync(key, value, expiry, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringSetAndGetAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<RedisValue> StringSetAndGetAsync(RedisKey key, RedisValue value, TimeSpan? expiry = null, bool keepTtl = false, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringSetAndGetAsync(key, value, expiry, keepTtl, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringSetAndGetAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<bool> StringSetAsync(RedisKey key, RedisValue value, TimeSpan? expiry, When when, CommandFlags flags)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringSetAsync(key, value, expiry, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringSetAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<bool> StringSetAsync(RedisKey key, RedisValue value, TimeSpan? expiry = null, bool keepTtl = false, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringSetAsync(key, value, expiry, keepTtl, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringSetAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public async Task<bool> StringSetAsync(KeyValuePair<RedisKey, RedisValue>[] values, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringSetAsync(values, when, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringSetAsync)} "); }
            }
            return await Task.FromResult(default(bool));
        }

        public bool StringSetBit(RedisKey key, long offset, bool bit, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringSetBit(key, offset, bit, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringSetBit)} {key}"); }
            }
            return default(bool);
        }

        public async Task<bool> StringSetBitAsync(RedisKey key, long offset, bool bit, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringSetBitAsync(key, offset, bit, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringSetBitAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public RedisValue StringSetRange(RedisKey key, long offset, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.StringSetRange(key, offset, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringSetRange)} {key}"); }
            }
            return default(bool);
        }

        public async Task<RedisValue> StringSetRangeAsync(RedisKey key, long offset, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            if (this._GetDatabase(out var database))
            {
                try { return await database.StringSetRangeAsync(key, offset, value, flags); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(StringSetRangeAsync)} {key}"); }
            }
            return await Task.FromResult(default(bool));
        }

        public bool TryWait(Task task)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.TryWait(task); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(TryWait)} "); }
            }
            return default(bool);
        }

        public void Wait(Task task)
        {
            if (this._GetDatabase(out var database))
            {
                try { database.Wait(task); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(Wait)} "); }
            }
        }

        public T Wait<T>(Task<T> task)
        {
            if (this._GetDatabase(out var database))
            {
                try { return database.Wait(task); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(Wait)} "); }
            }
            return default(T);
        }

        public void WaitAll(params Task[] tasks)
        {
            if (this._GetDatabase(out var database))
            {
                try { database.WaitAll(tasks); }
                catch (Exception ex) { CloseConnection(ex, $"{nameof(WaitAll)} "); }
            }
        }
    }
}