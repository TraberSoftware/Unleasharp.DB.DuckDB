using DuckDB;
using DuckDB.NET.Data;
using System;
using System.Data;
using Unleasharp.DB.Base.ExtensionMethods;
using Unleasharp.ExtensionMethods;

namespace Unleasharp.DB.DuckDB;

/// <summary>
/// Provides functionality for building and executing database queries using a DuckDB connection.
/// </summary>
/// <remarks>This class extends the base query builder functionality to support DuckDB-specific query execution. It
/// allows for the construction, execution, and management of queries, including support for synchronous and
/// asynchronous operations. The class handles query preparation, parameter binding, and result processing.</remarks>
public class QueryBuilder : Base.QueryBuilder<QueryBuilder, Connector, Query, DuckDBConnection, DuckDBConnectionStringBuilder> {
    /// <inheritdoc />
    public QueryBuilder(Connector dbConnector) : base(dbConnector) { }

    /// <inheritdoc />
    public QueryBuilder(Connector dbConnector, Query query) : base(dbConnector, query) { }

    #region Query execution
    /// <inheritdoc />
    protected override bool _Execute() {
        this.DBQuery.RenderPrepared();

        try {
            using (DuckDBCommand queryCommand = new DuckDBCommand(this.DBQuery.QueryPreparedString, this.Connector.Connection)) {
                this._PrepareDbCommand(queryCommand);

                switch (this.DBQuery.QueryType) {
                    case Base.QueryBuilding.QueryType.COUNT:
                        if (queryCommand.ExecuteScalar().TryConvert<int>(out int scalarCount)) {
                            this.TotalCount = scalarCount;
                        }
                        break;
                    case Base.QueryBuilding.QueryType.SELECT:
                    case Base.QueryBuilding.QueryType.SELECT_UNION:
                        using (DuckDBDataReader queryReader = queryCommand.ExecuteReader()) {
                            this._HandleQueryResult(queryReader);
                        }
                        break;
                    case Base.QueryBuilding.QueryType.INSERT:
                        if (this.DBQuery.QueryValues.Count == 1) {
                            this.LastInsertedId = queryCommand.ExecuteScalar();
                            this.AffectedRows   = this.LastInsertedId != null ? 1 : 0;
                        }
                        else {
                            this.AffectedRows = queryCommand.ExecuteNonQuery();
                        }
                        break;
                    case Base.QueryBuilding.QueryType.UPDATE:
                    default:
                        this.AffectedRows = queryCommand.ExecuteNonQuery();
                        break;
                }

                return true;
            }
        }
        catch (Exception ex) {
            this._OnQueryException(ex);
        }

        return false;
    }

    /// <inheritdoc />
    protected override T _ExecuteScalar<T>() {
        this.DBQuery.RenderPrepared();

        try {
            using (DuckDBCommand queryCommand = new DuckDBCommand(this.DBQuery.QueryPreparedString, this.Connector.Connection)) {
                this._PrepareDbCommand(queryCommand);
                if (queryCommand.ExecuteScalar().TryConvert<T>(out T scalarResult)) {
                    return scalarResult;
                }
            }
        }
        catch (Exception ex) {
            this._OnQueryException(ex);
        }

        return default(T);
    }

    /// <summary>
    /// Prepares the specified <see cref="DuckDBCommand"/> by adding parameters based on the query's prepared data.
    /// </summary>
    /// <remarks>If a value in the prepared data is an <see cref="Enum"/>, its description (retrieved via a
    /// custom extension method) is used as the parameter value. Otherwise, the value itself is used. After all
    /// parameters are added, the command is prepared by calling <see cref="DuckDBCommand.Prepare"/>.</remarks>
    /// <param name="queryCommand">The <see cref="DuckDBCommand"/> to be prepared. This command will have its parameters populated using the keys
    /// and values from the query's prepared data.</param>
    private void _PrepareDbCommand(DuckDBCommand queryCommand) {
        foreach (string queryPreparedDataKey in this.DBQuery.QueryPreparedData.Keys) {
            object? value = this.DBQuery.QueryPreparedData[queryPreparedDataKey].Value;

            if (value != null && value is DuckDBParameter) {
                DuckDBParameter sqlParameter = (DuckDBParameter)value;
                sqlParameter.ParameterName = queryPreparedDataKey.Substring(1);

                queryCommand.Parameters.Add(value);
                continue;
            }
            else {
                try {
                    DbType dbType = this.DBQuery.GetSystemDBType(value.GetType());
                    queryCommand.Parameters.Add(new DuckDBParameter {
                        ParameterName = queryPreparedDataKey.Substring(1),
                        Value         = value,
                        DbType        = dbType
                    });
                    continue;
                }
                catch (Exception ex) { }
            }
            queryCommand.Parameters.Add(new DuckDBParameter(queryPreparedDataKey.Substring(1), value));
        }
        queryCommand.Prepare();
    }

    /// <summary>
    /// Processes the result of a SQL query and populates a <see cref="DataTable"/> with the retrieved data.
    /// </summary>
    /// <remarks>This method creates a new <see cref="DataTable"/> and populates it with the data from the
    /// provided  <see cref="DuckDBDataReader"/>. Column names in the resulting <see cref="DataTable"/> are made unique 
    /// by appending a suffix if necessary. If the query result includes base table and column information,  column
    /// names are prefixed with the base table name in the format "BaseTable::BaseColumn".</remarks>
    /// <param name="queryReader">A <see cref="DuckDBDataReader"/> instance containing the query result to process.  The reader must be positioned
    /// at the start of the result set.</param>
    private void _HandleQueryResult(DuckDBDataReader queryReader) {
        this.Result = new DataTable();

        // Get schema information for all columns
        DataTable schemaTable = queryReader.GetSchemaTable();

        // Build column list with unique names
        for (int i = 0; i < queryReader.FieldCount; i++) {
            DataRow schemaRow = schemaTable.Rows[i];

            string safeName = (string)schemaRow["ColumnName"];     // Alias (or same as base if no alias)

            // If still duplicated, add a suffix
            string finalName = safeName;
            int suffix = 1;
            while (this.Result.Columns.Contains(finalName)) {
                finalName = $"{safeName}_{suffix++}";
            }

            this.Result.Columns.Add(new DataColumn(finalName, queryReader.GetFieldType(i)));
        }

        // Load rows
        object[] rowData = new object[this.Result.Columns.Count];
        this.Result.BeginLoadData();
        while (queryReader.Read()) {
            queryReader.GetValues(rowData);
            this.Result.LoadDataRow(rowData, true);

            rowData = new object[this.Result.Columns.Count]; // reset
        }
        this.Result.EndLoadData();
    }
    #endregion
}
