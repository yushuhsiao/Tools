using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Dapper
{
    [DebuggerStepThrough]
    public static class SqlMapperExtensions
    {
        static SqlMapperExtensions() => Init();

        public static void Init()
        {
            SqlMapper.TypeMapProvider = type => new DefaultTypeMap(type);
            SqlMapper.AddTypeHandler(new DateTimeHandler());
            SqlMapper.AddTypeHandler(new DateTimeOffsetHandler());
        }
        private class DateTimeHandler : SqlMapper.TypeHandler<DateTime>
        {
            public override void SetValue(IDbDataParameter parameter, DateTime value)
            {
                parameter.Value = value;
            }

            public override DateTime Parse(object value)
            {
                return DateTime.SpecifyKind((DateTime)value, DateTimeKind.Utc);
            }
        }
        private class DateTimeOffsetHandler : SqlMapper.TypeHandler<DateTimeOffset>
        {
            public override void SetValue(IDbDataParameter parameter, DateTimeOffset value)
            {
                parameter.Value = value;
            }

            public override DateTimeOffset Parse(object value)
            {
                var t = DateTime.SpecifyKind((DateTime)value, DateTimeKind.Utc);
                return new DateTimeOffset(t);
            }
        }

        public static int Execute(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.Execute(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static Task<int> ExecuteAsync(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.ExecuteAsync(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static IDataReader ExecuteReader(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.ExecuteReader(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static Task<IDataReader> ExecuteReaderAsync(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.ExecuteReaderAsync(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static T ExecuteScalar<T>(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.ExecuteScalar<T>(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static object ExecuteScalar(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.ExecuteScalar(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static Task<T> ExecuteScalarAsync<T>(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.ExecuteScalarAsync<T>(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static Task<object> ExecuteScalarAsync(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.ExecuteScalarAsync(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(this IDbTransaction tran, string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.Query(tran.Connection, sql, map, param, tran, buffered, splitOn, commandTimeout, commandType);
        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(this IDbTransaction tran, string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.Query(tran.Connection, sql, map, param, tran, buffered, splitOn, commandTimeout, commandType);
        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(this IDbTransaction tran, string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.Query(tran.Connection, sql, map, param, tran, buffered, splitOn, commandTimeout, commandType);
        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TReturn>(this IDbTransaction tran, string sql, Func<TFirst, TSecond, TThird, TFourth, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.Query(tran.Connection, sql, map, param, tran, buffered, splitOn, commandTimeout, commandType);
        public static IEnumerable<dynamic> Query(this IDbTransaction tran, string sql, object param = null, bool buffered = true, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.Query(tran.Connection, sql, param, tran, buffered, commandTimeout, commandType);
        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TReturn>(this IDbTransaction tran, string sql, Func<TFirst, TSecond, TThird, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.Query(tran.Connection, sql, map, param, tran, buffered, splitOn, commandTimeout, commandType);
        public static IEnumerable<TReturn> Query<TFirst, TSecond, TReturn>(this IDbTransaction tran, string sql, Func<TFirst, TSecond, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.Query(tran.Connection, sql, map, param, tran, buffered, splitOn, commandTimeout, commandType);
        public static IEnumerable<T> Query<T>(this IDbTransaction tran, string sql, object param = null, bool buffered = true, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.Query<T>(tran.Connection, sql, param, tran, buffered, commandTimeout, commandType);
        public static IEnumerable<object> Query(this IDbTransaction tran, Type type, string sql, object param = null, bool buffered = true, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.Query(tran.Connection, type, sql, param, tran, buffered, commandTimeout, commandType);
        public static IEnumerable<TReturn> Query<TReturn>(this IDbTransaction tran, string sql, Type[] types, Func<object[], TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.Query(tran.Connection, sql, types, map, param, tran, buffered, splitOn, commandTimeout, commandType);
        public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TReturn>(this IDbTransaction tran, string sql, Func<TFirst, TSecond, TThird, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryAsync(tran.Connection, sql, map, param, tran, buffered, splitOn, commandTimeout, commandType);
        public static Task<IEnumerable<T>> QueryAsync<T>(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryAsync<T>(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TReturn>(this IDbTransaction tran, string sql, Func<TFirst, TSecond, TThird, TFourth, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryAsync(tran.Connection, sql, map, param, tran, buffered, splitOn, commandTimeout, commandType);
        public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(this IDbTransaction tran, string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryAsync(tran.Connection, sql, map, param, tran, buffered, splitOn, commandTimeout, commandType);
        public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(this IDbTransaction tran, string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryAsync(tran.Connection, sql, map, param, tran, buffered, splitOn, commandTimeout, commandType);
        public static Task<IEnumerable<object>> QueryAsync(this IDbTransaction tran, Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryAsync(tran.Connection, type, sql, param, tran, commandTimeout, commandType);
        public static Task<IEnumerable<dynamic>> QueryAsync(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryAsync(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static Task<IEnumerable<TReturn>> QueryAsync<TReturn>(this IDbTransaction tran, string sql, Type[] types, Func<object[], TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryAsync(tran.Connection, sql, types, map, param, tran, buffered, splitOn, commandTimeout, commandType);
        public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(this IDbTransaction tran, string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryAsync(tran.Connection, sql, map, param, tran, buffered, splitOn, commandTimeout, commandType);
        public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TReturn>(this IDbTransaction tran, string sql, Func<TFirst, TSecond, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryAsync(tran.Connection, sql, map, param, tran, buffered, splitOn, commandTimeout, commandType);
        public static object QueryFirst(this IDbTransaction tran, Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryFirst(tran.Connection, type, sql, param, tran, commandTimeout, commandType);
        public static T QueryFirst<T>(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryFirst<T>(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static dynamic QueryFirst(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryFirst(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static Task<T> QueryFirstAsync<T>(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryFirstAsync<T>(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static Task<dynamic> QueryFirstAsync(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryFirstAsync(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static Task<object> QueryFirstAsync(this IDbTransaction tran, Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryFirstAsync(tran.Connection, type, sql, param, tran, commandTimeout, commandType);
        public static dynamic QueryFirstOrDefault(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryFirstOrDefault(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static T QueryFirstOrDefault<T>(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryFirstOrDefault<T>(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static object QueryFirstOrDefault(this IDbTransaction tran, Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryFirstOrDefault(tran.Connection, type, sql, param, tran, commandTimeout, commandType);
        public static Task<object> QueryFirstOrDefaultAsync(this IDbTransaction tran, Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryFirstOrDefaultAsync(tran.Connection, type, sql, param, tran, commandTimeout, commandType);
        public static Task<dynamic> QueryFirstOrDefaultAsync(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryFirstOrDefaultAsync(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static Task<T> QueryFirstOrDefaultAsync<T>(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryFirstOrDefaultAsync<T>(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static SqlMapper.GridReader QueryMultiple(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryMultiple(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static Task<SqlMapper.GridReader> QueryMultipleAsync(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QueryMultipleAsync(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static object QuerySingle(this IDbTransaction tran, Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QuerySingle(tran.Connection, type, sql, param, tran, commandTimeout, commandType);
        public static T QuerySingle<T>(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QuerySingle<T>(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static dynamic QuerySingle(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QuerySingle(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static Task<T> QuerySingleAsync<T>(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QuerySingleAsync<T>(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static Task<dynamic> QuerySingleAsync(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QuerySingleAsync(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static Task<object> QuerySingleAsync(this IDbTransaction tran, Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QuerySingleAsync(tran.Connection, type, sql, param, tran, commandTimeout, commandType);
        public static dynamic QuerySingleOrDefault(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QuerySingleOrDefault(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static T QuerySingleOrDefault<T>(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QuerySingleOrDefault<T>(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static object QuerySingleOrDefault(this IDbTransaction tran, Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QuerySingleOrDefault(tran.Connection, type, sql, param, tran, commandTimeout, commandType);
        public static Task<object> QuerySingleOrDefaultAsync(this IDbTransaction tran, Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QuerySingleOrDefaultAsync(tran.Connection, type, sql, param, tran, commandTimeout, commandType);
        public static Task<dynamic> QuerySingleOrDefaultAsync(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QuerySingleOrDefaultAsync(tran.Connection, sql, param, tran, commandTimeout, commandType);
        public static Task<T> QuerySingleOrDefaultAsync<T>(this IDbTransaction tran, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => SqlMapper.QuerySingleOrDefaultAsync<T>(tran.Connection, sql, param, tran, commandTimeout, commandType);
    }
}