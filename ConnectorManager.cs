using DuckDB;
using DuckDB.NET.Data;
using System;
using Unleasharp.DB.Base;

namespace Unleasharp.DB.DuckDB;

/// <summary>
/// Manager class for DuckDB database connections that provides access to query builders
/// for constructing and executing SQL queries.
/// </summary>
public class ConnectorManager : 
    ConnectorManager<ConnectorManager, Connector, DuckDBConnectionStringBuilder, DuckDBConnection, QueryBuilder, Query>
{
    #region Default constructors
    /// <inheritdoc />
    public ConnectorManager() : base() { }

    /// <inheritdoc />
    public ConnectorManager(DuckDBConnectionStringBuilder stringBuilder) : base(stringBuilder)    { }

    /// <inheritdoc />
    public ConnectorManager(string connectionString)                     : base(connectionString) { }
    #endregion
}
