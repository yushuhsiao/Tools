using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System.Collections.Generic;
using System.Threading;
//using System.Data.SqlClient;
using _DebuggerStepThrough = System.Diagnostics.FakeDebuggerStepThroughAttribute;

namespace System.Data
{
    [_DebuggerStepThrough]
    public static class DbConnectionPooling
    {
        static Dictionary<string, AsyncLocal<_DbConn>> _dbConn = new Dictionary<string, AsyncLocal<_DbConn>>();

        public static void ResetDbConnection(this DbConnectionString connectionString, IServiceProvider service)
        {
            var httpContext = service.GetService<IHttpContextAccessor>().HttpContext;
            if (httpContext == null)
            {
                AsyncLocal<_DbConn> state;
                lock (_dbConn)
                    if (_dbConn.TryGetValue(connectionString.Value, out state) == false)
                        return;
                using (var conn = state.Value)
                {
                    if (conn != null)
                    {
                        conn.AllowDispose = true;
                        state.Value = null;
                    }
                }
            }
            else
            {
                object key = typeof(_DbConn).AssemblyQualifiedName + connectionString.Value;
                if (httpContext.Items.TryGetValue(key, out var obj))
                    using (obj as _DbConn)
                        httpContext.Items.Remove(key);
            }
        }

        public static IDbConnection GetDbConnection(this DbConnectionString connectionString, IServiceProvider service, Func<DbConnectionString, IDbConnection> createDbConnection)
        {
            var httpContext = service.GetService<IHttpContextAccessor>()?.HttpContext;
            if (httpContext == null)
            {
                AsyncLocal<_DbConn> state;
                lock (_dbConn)
                    if (_dbConn.TryGetValue(connectionString.Value, out state) == false)
                        _dbConn[connectionString.Value] = state = new AsyncLocal<_DbConn>();
                if (state.Value == null)
                {
                    var conn = createDbConnection(connectionString);
                    state.Value = new _DbConn(conn);
                }
                return state.Value;
            }
            else
            {
                object key = typeof(_DbConn).AssemblyQualifiedName + connectionString.Value;
                if (httpContext.Items.TryGetValue(key, out var obj))
                    return ((_State)obj).Conn;
                else
                {
                    var conn = createDbConnection(connectionString);
                    var state = new _State(conn);
                    httpContext.Items[key] = state;
                    httpContext.Response.RegisterForDispose(state);
                    return state.Conn;
                }
            }
        }

        private class _State : IDisposable
        {
            public _DbConn Conn { get; private set; }

            public _State(IDbConnection connection)
            {
                this.Conn = new _DbConn(connection);
            }

            void IDisposable.Dispose()
            {
                using (var conn = this.Conn)
                {
                    if (conn != null)
                    {
                        conn.AllowDispose = true;
                        this.Conn = null;
                    }
                }
            }
        }

        private class _DbConn : IDbConnection
        {
            public bool AllowDispose { get; set; } = false;
            private IDbConnection _conn;

            public _DbConn(IDbConnection conn)
            {
                _conn = conn;
            }


            void IDisposable.Dispose()
            {
                if (this.AllowDispose)
                    using (var conn = this._conn)
                        this._conn = null;
            }

            string IDbConnection.ConnectionString
            {
                get => _conn.ConnectionString;
                set => _conn.ConnectionString = value;
            }

            int IDbConnection.ConnectionTimeout => _conn.ConnectionTimeout;

            string IDbConnection.Database => _conn.Database;

            ConnectionState IDbConnection.State => _conn.State;

            IDbTransaction IDbConnection.BeginTransaction() => _conn.BeginTransaction();

            IDbTransaction IDbConnection.BeginTransaction(IsolationLevel il) => _conn.BeginTransaction(il);

            void IDbConnection.ChangeDatabase(string databaseName) => _conn.ChangeDatabase(databaseName);

            void IDbConnection.Close() => _conn.Close();

            IDbCommand IDbConnection.CreateCommand() => _conn.CreateCommand();

            void IDbConnection.Open() => _conn.Open();
        }

        private class _DbCommand : IDbCommand
        {
            private IDbCommand _cmd;

            public _DbCommand(IDbCommand cmd)
            {
                _cmd = cmd;
            }

            public string CommandText { get => _cmd.CommandText; set => _cmd.CommandText = value; }
            public int CommandTimeout { get => _cmd.CommandTimeout; set => _cmd.CommandTimeout = value; }
            public CommandType CommandType { get => _cmd.CommandType; set => _cmd.CommandType = value; }
            public IDbConnection Connection { get => _cmd.Connection; set => _cmd.Connection = value; }

            public IDataParameterCollection Parameters => _cmd.Parameters;

            public IDbTransaction Transaction { get => _cmd.Transaction; set => _cmd.Transaction = value; }
            public UpdateRowSource UpdatedRowSource { get => _cmd.UpdatedRowSource; set => _cmd.UpdatedRowSource = value; }

            public void Cancel()
            {
                _cmd.Cancel();
            }

            public IDbDataParameter CreateParameter()
            {
                return _cmd.CreateParameter();
            }

            public void Dispose()
            {
                _cmd.Dispose();
            }

            public int ExecuteNonQuery()
            {
                return _cmd.ExecuteNonQuery();
            }

            public IDataReader ExecuteReader()
            {
                return _cmd.ExecuteReader();
            }

            public IDataReader ExecuteReader(CommandBehavior behavior)
            {
                return _cmd.ExecuteReader(behavior);
            }

            public object ExecuteScalar()
            {
                return _cmd.ExecuteScalar();
            }

            public void Prepare()
            {
                _cmd.Prepare();
            }
        }
    }
}