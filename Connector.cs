using DuckDB.NET.Data;
using System;
using Unleasharp.DB.Base;

namespace Unleasharp.DB.DuckDB;

/// <summary>
/// Represents a connector for managing connections to a DuckDB database.
/// </summary>
/// <remarks>This class provides functionality to establish, manage, and terminate connections to a DuckDB
/// database. It extends the base functionality provided by <see cref="Unleasharp.DB.Base.Connector{TConnector,
/// TConnectionStringBuilder}"/>. Use this class to interact with a DuckDB database by providing a connection string or a
/// pre-configured <see cref="DuckDBConnection"/>.</remarks>
public class Connector : Unleasharp.DB.Base.Connector<Connector, DuckDBConnection, DuckDBConnectionStringBuilder> {
    #region Default constructors
    /// <inheritdoc />
    public Connector(DuckDBConnectionStringBuilder stringBuilder) : base(stringBuilder)    { }
    /// <inheritdoc />
    public Connector(string connectionString) { 
        this.StringBuilder = new DuckDBConnectionStringBuilder {
            ConnectionString = connectionString
        };
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="Connector"/> class using the specified DuckDB connection.
    /// </summary>
    /// <param name="connection">The <see cref="DuckDBConnection"/> instance to be used by the connector. Cannot be <see langword="null"/>.</param>
    public Connector(DuckDBConnection connection) {
        this.Connection = connection;
    }
    #endregion
}
