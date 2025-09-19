using DuckDB.NET.Data;
using DuckDB.NET.Native;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Unleasharp.DB.Base;
using Unleasharp.DB.Base.ExtensionMethods;
using Unleasharp.DB.Base.QueryBuilding;
using Unleasharp.DB.Base.SchemaDefinition;
using Unleasharp.DB.DuckDB.Functions;
using Unleasharp.ExtensionMethods;

namespace Unleasharp.DB.DuckDB;

/// <summary>
/// DuckDB-specific query builder that extends the generic <see cref="Unleasharp.DB.Base.Query{Query}"/>.
/// Provides DuckDB syntax and rendering for SQL statements.
/// </summary>
public class Query : Unleasharp.DB.Base.Query<Query> {
    /// <inheritdoc/>
    protected override DatabaseEngine _Engine                 { get { return DatabaseEngine.DuckDB;    } }

    /// <inheritdoc/>
    protected override string         _QueryPreparationPrefix { get { return "$prepared_query_value_"; } }

    /// <summary>
    /// The delimiter used for escaping field names in DuckDB.
    /// </summary>
    public const string FieldDelimiter = "\"";
    /// <summary>
    /// The delimiter used for escaping values in DuckDB.
    /// </summary>
    public const string ValueDelimiter = "'";


    #region Custom DuckDB query data
    /// <summary>
    /// Gets or sets the target table for a COPY ... FROM ... query.
    /// </summary>
    public From<Query> QueryCopyInto { get; set; }

    /// <summary>
    /// Specifies which column value should be returned from DuckDB when performing an INSERT statement with a RETURNING clause.
    /// For example, in "INSERT INTO ... RETURNING id;", this field indicates which column (e.g., "id") to return from the inserted rows.
    /// </summary>
    public string QueryReturning {
        get; protected set;
    }
    #endregion

    #region Custom DuckDB query building methods
    /// <summary>
    /// Sets the target of a COPY &lt;target&gt; FROM &lt;source&gt; statement.
    /// </summary>
    /// <param name="copy">
    /// A <see cref="From{T}"/> fragment that describes the COPY destination.
    /// </param>
    /// <returns>
    /// The current <see cref="Query"/> instance to allow fluent chaining.
    /// </returns>
    public Query CopyInto(From<Query> copy) {
        this.QueryCopyInto = copy;

        this.Touch();
        return this;
    }

    /// <summary>
    /// Copies data from a CSV source into a database table represented by the specified class type.
    /// </summary>
    /// <remarks>This method renders a read_csv function as the data source for a COPY ... FROM read_csv('file.csv') query.</remarks>
    /// <param name="tableName">The table name.</param>
    /// <param name="readCsvFunction">A function that provides the CSV data to be copied.</param>
    /// <returns>The current query instance.</returns>
    public Query CopyIntoFromCSV(string tableName, ReadCSVFunction readCsvFunction) { 
        this.QueryType = QueryType.INSERT;

        this.From(readCsvFunction.AsCopyFrom(), false);
        return this.CopyInto(new From<Query> {
            Table       = tableName,
            EscapeTable = true,
        });
    }

    /// <summary>
    /// Copies data from a CSV source into a database table represented by the specified class type.
    /// </summary>
    /// <remarks>This method renders a read_csv function as the data source for a COPY ... FROM read_csv('file.csv') query.</remarks>
    /// <typeparam name="TableClass">The table type.</typeparam>
    /// <param name="readCsvFunction">A function that provides the CSV data to be copied.</param>
    /// <returns>The current query instance.</returns>
    public Query CopyIntoFromCSV<TableClass>(ReadCSVFunction readCsvFunction) where TableClass : class { 
        string tableName      = typeof(TableClass).Name;
        Table  tableAttribute = typeof(TableClass).GetCustomAttribute<Table>();
        if (tableAttribute != null) {
            tableName = tableAttribute.Name;
        }
        
        return this.CopyIntoFromCSV(tableName, readCsvFunction);
    }

    /// <summary>
    /// Specifies the data source for the query using the provided CSV reading function.
    /// </summary>
    /// <param name="readCsv">The <see cref="ReadCSVFunction"/> instance that defines how to read the CSV data.</param>
    /// <returns>The current query instance.</returns>
    public Query From(ReadCSVFunction readCsv) {
        this.From(readCsv.AsFunction(), false);

        return this;
    }
    #endregion

    #region Public query building methods overrides
    /// <inheritdoc/>
    public override Query Into<TableClass>() {
        foreach (MemberInfo member in typeof(TableClass).GetMembers()) {
            Column column = member.GetCustomAttribute<Column>();
            if (column != null) {
                if (column.PrimaryKey) {
                    this.QueryReturning = column.Name;
                }
            }
        }

        return base.Into<TableClass>();
    }

    /// <inheritdoc/>
    public override Query From<TableClass>() {
        foreach (MemberInfo member in typeof(TableClass).GetMembers()) {
            Column column = member.GetCustomAttribute<Column>();
            if (column != null) {
                if (column.PrimaryKey) {
                    this.QueryReturning = column.Name;
                }
            }
        }

        return base.From<TableClass>();
    }

    /// <inheritdoc/>
    public override Query Set<T>(Expression<Func<T, object>> expression, dynamic value, bool escape = true) {
        Type   tableType         = typeof(T);
        string classPropertyName = ExpressionHelper.ExtractClassMemberName<T>(expression);
        string dbColumnName      = ReflectionCache.GetColumnName<T>(expression);

        MemberInfo member = tableType.GetMember(classPropertyName).FirstOrDefault();

        if (member == null) {
            return this.Set(dbColumnName, value, escape);
        }

        DuckDBParameter param = this.__GetMemberInfoDuckDBParameter(value, member);
        if (param != null) {
            return this.Set(new Where<Query> {
                Field       = new FieldSelector(dbColumnName),
                Value       = param,
                EscapeValue = true
            });
        }

        return this;
    }

    /// <inheritdoc/>
    public override Query Value<T>(T row, bool skipNullValues = true) where T : class {
        Type rowType = row.GetType();

        if (rowType.IsClass) {
            List<DuckDBParameter> rowValues = new List<DuckDBParameter>();

            foreach (FieldInfo field in rowType.GetFields()) {
                DuckDBParameter parameter = this.__GetMemberInfoDuckDBParameter(field.GetValue(row), field);
                if (parameter != null) {
                    rowValues.Add(parameter);
                }
            }
            foreach (PropertyInfo property in rowType.GetProperties()) {
                DuckDBParameter parameter = this.__GetMemberInfoDuckDBParameter(property.GetValue(row), property);
                if (parameter != null) {
                    rowValues.Add(parameter);
                }
            }

            return this.Value(
                rowValues
                    .Where(rowValue => rowValue != null)
                    .ToDictionary(
                        rowValue => rowValue.ParameterName, 
                        rowValue => rowValue as dynamic
                    )
            );
        }

        return this.Value(row.ToDynamicDictionaryForInsert());
    }

    /// <inheritdoc/>
    public override Query Values<T>(List<T> rows, bool skipNullValues = true) where T : class {
        foreach (T row in rows) {
            this.Value<T>(row, skipNullValues);
        }

        return (Query) this;
    }

    /// <summary>
    /// Creates an <see cref="DuckDBParameter"/> for the specified member of a class, using metadata from the provided
    /// <see cref="MemberInfo"/>.
    /// </summary>
    /// <remarks>This method uses metadata from the <see cref="MemberInfo"/> to configure the parameter,
    /// including attributes such as <see cref="ColumnAttribute"/>. If the member is nullable, the underlying type is
    /// used for type determination. The method also maps the member's type to the appropriate DuckDB data
    /// type.</remarks>
    /// <param name="value">The value to be assigned to the parameter. If <paramref name="value"/> is <see langword="null"/>, it will be
    /// replaced with <see cref="DBNull.Value"/>.</param>
    /// <param name="memberInfo">The <see cref="MemberInfo"/> representing the class member for which the parameter is being created. This is
    /// used to determine the parameter's name, type, and additional metadata.</param>
    /// <returns>An <see cref="DuckDBParameter"/> configured with the appropriate name, value, and type based on the provided
    /// <paramref name="memberInfo"/> and its attributes. If the member is a primary key with a non-null constraint and
    /// the value is <see langword="null"/>, the method returns <see langword="null"/>.</returns>
    private DuckDBParameter __GetMemberInfoDuckDBParameter(object? value, MemberInfo memberInfo) {
        string          classFieldName   = memberInfo.Name;
        string          dbFieldName      = classFieldName;
        Type            memberInfoType   = memberInfo.GetDataType();
        Column?         column           = memberInfo.GetCustomAttribute<Column>();
        ColumnDataType? columnDataType   = null;
        DbType?         dbColumnDataType = null;

        if (memberInfo.IsSystemColumn()) {
            return null;
        }

        if (value == null) {
            value = DBNull.Value;
        }

        // Don't set null values to AutoIncrement columns
        if ((column != null && column.AutoIncrement) && (value == null || value == DBNull.Value)) {
            return null;
        }

        if (Nullable.GetUnderlyingType(memberInfoType) != null) {
            memberInfoType = Nullable.GetUnderlyingType(memberInfoType);
        }

        if (column != null) {
            dbFieldName = column.Name;

            if (!string.IsNullOrEmpty(column.DataTypeString)) {
                columnDataType = this.GetColumnDataType(column.DataTypeString);
            }
            if (column.DataType != null) {
                columnDataType = column.DataType;
            }

            if (columnDataType == null) {
                columnDataType = memberInfoType.GetColumnType();
            }

            dbColumnDataType = this.GetSystemDBType(columnDataType.Value.GetType());
        }

        if (dbColumnDataType.HasValue && !memberInfoType.IsEnum) {
            return new DuckDBParameter {
                ParameterName = dbFieldName,
                Value         = value,
                DbType        = dbColumnDataType.Value,
                SourceColumn  = dbFieldName,
            };
        }

        return new DuckDBParameter(dbFieldName, value);
    }
    #endregion

    #region Query building
    #region Query building - Create
    /// <summary>
    /// Creates a DuckDB ENUM type based on the specified .NET enum type.
    /// </summary>
    /// <remarks>The method generates a DuckDB ENUM type definition using the names and values of the
    /// provided .NET enum. The resulting query string is prepared and stored in the <c>QueryPreparedString</c>
    /// property.</remarks>
    /// <param name="enumType">The .NET <see cref="Type"/> representing the enum. Must be a valid enum type.</param>
    /// <returns>The current query instance.</returns>
    public Query CreateEnumType(Type enumType) {
        this.SetQueryType(QueryType.CREATE);

        List<string> enumValues = new List<string>();
        foreach (Enum enumValue in Enum.GetValues(enumType)) {
            enumValues.Add(this.__RenderWhereValue(enumValue.GetDescription(), true));
        }

        this.QueryPreparedString = $"CREATE TYPE {Query.FieldDelimiter}{enumType.Name.ToLowerInvariant()}{Query.FieldDelimiter} AS ENUM({string.Join(',', enumValues)})";
        this.QueryRenderedString = QueryPreparedString;

        this.Untouch();
        return this;
    }

    /// <summary>
    /// Creates a DuckDB SEQUENCE.
    /// </summary>
    /// <remarks>The method generates a DuckDB sequence number generator.</remarks>
    /// <param name="sequenceName">The name of the sequence.</param>
    /// <param name="start">The initial value of the sequence.</param>
    /// <param name="increment">The increment to apply to each sequencial value.</param>
    /// <param name="maxValue">The highest possible value of the sequence.</param>
    /// <param name="cycle">If enabled, allows cycling through the same sequence repeatedly.</param>
    /// <returns>The current query instance.</returns>
    public Query CreateSequence(string sequenceName, long start = 1, int increment = 1, long maxValue = -1, bool cycle = false) {
        this.SetQueryType(QueryType.CREATE);

        this.QueryPreparedString = $"CREATE SEQUENCE {sequenceName} START WITH {start} INCREMENT BY {increment}{(maxValue > 0 ? " {maxValue}" : "")}{(cycle ? " CYCLE" : "")}";
        this.QueryRenderedString = QueryPreparedString;

        this.Untouch();
        return this;
    }

    /// <summary>
    /// Alias for CreateEnumType(Type).
    /// </summary>
    /// <typeparam name="EnumType">The .NET <see cref="Type"/> representing the enum. Must be a type derived from 
    /// <see cref="System.Enum"/>.</typeparam>
    /// <returns>The current query instance.</returns>
    public Query CreateEnumType<EnumType>() where EnumType : Enum {
        return this.CreateEnumType(typeof(EnumType));
    }
    #endregion
    #endregion

    #region Query rendering
    #region Query fragment rendering
    /// <inheritdoc/>
    public override void _RenderPrepared() {
        this._Render();

        string rendered = this.QueryPreparedString;

        foreach (string preparedDataItemKey in this.QueryPreparedData.Keys.Reverse()) {
            PreparedValue preparedDataItemValue = this.QueryPreparedData[preparedDataItemKey];
            object?       value                 = preparedDataItemValue.Value is DuckDBParameter ? (((DuckDBParameter)preparedDataItemValue.Value).Value) : preparedDataItemValue.Value;
            bool          escape                = preparedDataItemValue.Value is DuckDBParameter ? true : preparedDataItemValue.EscapeValue;
            string        renderedValue         = "NULL";

            if (value != null && value != DBNull.Value) {
                renderedValue = true switch {
                    true when value is Enum   => this.__RenderWhereValue(((Enum)value).GetDescription(), escape),
                    true when value is byte[] => this.__RenderWhereValue($"0x{Convert.ToHexString((byte[])value)}", escape),
                                            _ => this.__RenderWhereValue(value, escape)
                };
            }
            rendered = rendered.Replace(preparedDataItemKey, renderedValue);
        }

        this.QueryRenderedString = rendered;
    }

    /// <summary>
    /// Renders a SQL SELECT fragment as a string, including subqueries and aliases if applicable.
    /// </summary>
    /// <remarks>This method handles both simple field rendering and subquery rendering. If the fragment
    /// includes a subquery, the subquery is rendered  in the context of the current query or its parent query, if
    /// available. If the fragment includes an alias, it is appended to the rendered  field using the "AS"
    /// keyword.</remarks>
    /// <param name="fragment">The <see cref="Select{T}"/> object representing the SQL SELECT fragment to render.</param>
    /// <returns>A string representation of the SQL SELECT fragment. If the fragment contains a subquery, the rendered subquery
    /// is enclosed in parentheses.  If an alias is specified, it is appended to the rendered field with the "AS"
    /// keyword.</returns>
    public string RenderSelect(Select<Query> fragment) {
        if (fragment.Subquery != null) {
            return "(" + fragment.Subquery.WithParentQuery(this.ParentQuery != null ? this.ParentQuery : this).Render() + ")";
        }

        return fragment.Field.Render() + (!string.IsNullOrWhiteSpace(fragment.Alias) ? $" AS {fragment.Alias}" : "");
    }

    /// <summary>
    /// Renders a SQL "FROM" clause based on the specified <see cref="From{T}"/> fragment.
    /// </summary>
    /// <remarks>This method supports rendering both subqueries and table names for use in SQL "FROM" clauses.
    /// If a subquery is provided, it is rendered with the appropriate parent query context. If a table name is
    /// provided, it can be optionally escaped using the <c>FieldDelimiter</c>.</remarks>
    /// <param name="fragment">The <see cref="From{T}"/> fragment containing the table, subquery, and alias information to be rendered into a
    /// SQL "FROM" clause. The <paramref name="fragment"/> must not be null.</param>
    /// <returns>A string representing the rendered SQL "FROM" clause. If the fragment contains a subquery, the subquery is
    /// rendered and enclosed in parentheses. If the fragment specifies a table, the table name is rendered, optionally
    /// escaped, and followed by the table alias if provided.</returns>
    public string RenderFrom(From<Query> fragment) {
        if (fragment.Subquery != null) {
            return $"({fragment.Subquery.WithParentQuery(this.ParentQuery != null ? this.ParentQuery : this).Render()}){(!string.IsNullOrWhiteSpace(fragment.TableAlias) ? $" {fragment.TableAlias}" : "")}";
        }

        string rendered = string.Empty;

        if (!string.IsNullOrWhiteSpace(fragment.Table)) {
            if (fragment.EscapeTable) {
                rendered = FieldDelimiter + fragment.Table + FieldDelimiter;
            }
            else {
                rendered = fragment.Table;
            }
        }

        return rendered + (fragment.TableAlias != string.Empty ? $" {fragment.TableAlias}" : "");
    }

    /// <summary>
    /// Renders a SQL JOIN clause as a string based on the specified join fragment.
    /// </summary>
    /// <remarks>The table name is optionally escaped based on the <see cref="Join{T}.EscapeTable"/> property.
    /// The ON condition is rendered using the <see cref="RenderWhere"/> method.</remarks>
    /// <param name="fragment">The <see cref="Join{T}"/> object representing the join details, including the table name,  whether the table
    /// name should be escaped, and the join condition.</param>
    /// <returns>A string representing the SQL JOIN clause, including the table name and the ON condition.</returns>
    public string RenderJoin(Join<Query> fragment) {
        return $"{(fragment.EscapeTable ? FieldDelimiter + fragment.Table + FieldDelimiter : fragment.Table)} ON {this.RenderWhere(fragment.Condition)}";
    }

    /// <summary>
    /// Renders a SQL "GROUP BY" clause based on the specified <see cref="GroupBy"/> fragment.
    /// </summary>
    /// <remarks>The method constructs the "GROUP BY" clause by combining the table and field names specified
    /// in the <paramref name="fragment"/>. If either the table or field name is null, empty, or consists only of
    /// whitespace, it will be excluded from the output. Escaping is applied to the table and field names if the
    /// <c>Escape</c> property of the <paramref name="fragment"/> is set to <see langword="true"/>.</remarks>
    /// <param name="fragment">The <see cref="GroupBy"/> fragment containing the table and field information to be rendered.</param>
    /// <returns>A string representing the "GROUP BY" clause, formatted with the table and field names. If the table or field
    /// names are marked for escaping, they will be enclosed with the appropriate delimiters.</returns>
    public string RenderGroupBy(GroupBy fragment) {
        List<string> toRender = new List<string>();

        if (!string.IsNullOrWhiteSpace(fragment.Field.Table)) {
            if (fragment.Field.Escape) {
                toRender.Add(FieldDelimiter + fragment.Field.Table + FieldDelimiter);
            }
            else {
                toRender.Add(fragment.Field.Table);
            }
        }

        if (!string.IsNullOrWhiteSpace(fragment.Field.Field)) {
            if (fragment.Field.Escape) {
                toRender.Add(FieldDelimiter + fragment.Field.Field + FieldDelimiter);
            }
            else {
                toRender.Add(fragment.Field.Field);
            }
        }

        return String.Join('.', toRender);
    }

    /// <summary>
    /// Renders a SQL WHERE clause based on the specified query fragment.
    /// </summary>
    /// <remarks>This method generates a SQL WHERE clause by combining the field, comparer, and value  (or
    /// value field) specified in the <paramref name="fragment"/>. If a subquery is provided,  it is rendered and
    /// included in the output. Special handling is applied for null values  and escaping, depending on the fragment's
    /// configuration.</remarks>
    /// <param name="fragment">The <see cref="Where{T}"/> object representing the query fragment to render.  This includes the field, comparer,
    /// value, and optional subquery or value field.</param>
    /// <returns>A <see cref="string"/> containing the rendered SQL WHERE clause.  The result is formatted based on the provided
    /// fragment's properties, including  handling subqueries, null values, and value escaping as necessary.</returns>
    public string RenderWhere(Where<Query> fragment) {
        if (fragment.Subquery != null) {
            return $"{fragment.Field.Render()} {fragment.Comparer.GetDescription()} ({fragment.Subquery.WithParentQuery(this.ParentQuery != null ? this.ParentQuery : this).Render()})";
        }

        List<string> toRender = new List<string>();

        toRender.Add(fragment.Field.Render());

        // We are comparing fields, not values
        if (fragment.ValueField != null) {
            toRender.Add(fragment.ValueField.Render());
        }
        else {
            if (fragment.Value == null) {
                fragment.Comparer = WhereComparer.IS;
                toRender.Add("NULL");
            }
            else {
                if (fragment.EscapeValue) {
                    toRender.Add(this.PrepareQueryValue(fragment.Value, fragment.EscapeValue));
                }
                else {
                    toRender.Add(this.__RenderWhereValue(fragment.Value, false));
                }
            }
        }

        return String.Join(fragment.Comparer.GetDescription(), toRender);
    }

    /// <summary>
    /// Renders a SQL "WHERE IN" clause based on the specified fragment.
    /// </summary>
    /// <remarks>If the <paramref name="fragment"/> contains a subquery, the method renders the "WHERE IN"
    /// clause using the subquery. Otherwise, it renders the clause using the provided values, applying value escaping
    /// if specified by the fragment.</remarks>
    /// <param name="fragment">The <see cref="WhereIn{T}"/> fragment containing the field, values, and optional subquery to be rendered into
    /// the "WHERE IN" clause.</param>
    /// <returns>A string representing the rendered "WHERE IN" clause. Returns an empty string if the fragment's <see
    /// cref="WhereIn{T}.Values"/> collection is null or empty.</returns>
    public string RenderWhereIn(WhereIn<Query> fragment) {
        if (fragment.Subquery != null) {
            return $"{fragment.Field.Render()} {fragment.Comparer.GetDescription()} ({fragment.Subquery.WithParentQuery(this.ParentQuery != null ? this.ParentQuery : this).Render()})";
        }

        if (fragment.Values == null || fragment.Values.Count == 0) {
            return String.Empty;
        }

        List<string> toRender = new List<string>();

        foreach (dynamic fragmentValue in fragment.Values) {
            if (fragment.EscapeValue) {
                toRender.Add(this.PrepareQueryValue(fragmentValue, fragment.EscapeValue));
            }
            else {
                toRender.Add(__RenderWhereValue(fragmentValue, fragment.EscapeValue));
            }
        }

        return $"{fragment.Field.Render()} IN ({String.Join(",", toRender)})";
    }
    #endregion

    #region Query sentence rendering
    /// <inheritdoc/>
    protected override string _RenderCountSentence() {
        return "SELECT COUNT(*)";
    }

    /// <inheritdoc/>
    protected override string _RenderSelectSentence() {
        List<string> rendered = new List<string>();

        if (this.QuerySelect.Count > 0) {
            foreach (Select<Query> queryFragment in this.QuerySelect) {
                rendered.Add(this.RenderSelect(queryFragment));
            }
        }
        else {
            rendered.Add("*");
        }

        return rendered.Count > 0 ? $"SELECT {(this.QueryDistinct ? "DISTINCT " : "")}{string.Join(',', rendered)}" : "";
    }

    /// <inheritdoc/>
    protected override string _RenderUnionSentence() {
        List<string> rendered = new List<string>();

        if (this.QueryUnion.Count > 0) {
            foreach (Union<Query> union in this.QueryUnion) {
                if (rendered.Any()) {
                    rendered.Add(union.Type.GetDescription());
                }
                rendered.Add(union.Query.WithParentQuery(this.ParentQuery != null ? this.ParentQuery : this).Render());
            }
        }

        if (rendered.Count > 0) {
            if (!string.IsNullOrWhiteSpace(this.QueryUnionAlias)) {
                return $"FROM ({string.Join(" ", rendered)}) {this.QueryUnionAlias}";
            }
            return $"{string.Join(" ", rendered)}";
        }
        return string.Empty;
    }

    /// <inheritdoc/>
    protected override string _RenderFromSentence() {
        List<string> rendered = new List<string>();

        foreach (From<Query> queryFragment in this.QueryFrom) {
            rendered.Add(this.RenderFrom(queryFragment));
        }

        return (rendered.Count > 0 ? "FROM " + string.Join(',', rendered) : "");
    }

    /// <inheritdoc/>
    protected override string _RenderJoinSentence() {
        List<string> rendered = new List<string>();
        foreach (Join<Query> queryFragment in this.QueryJoin) {
            rendered.Add(this.RenderJoin(queryFragment));
        }

        return (rendered.Count > 0 ? "JOIN " + string.Join(',', rendered) : "");
    }

    /// <inheritdoc/>
    protected override string _RenderWhereSentence() {
        List<string> rendered = new List<string>();
        foreach (Where<Query> queryFragment in this.QueryWhere) {
            if (rendered.Any()) {
                rendered.Add(queryFragment.Operator.GetDescription());
            }
            rendered.Add(this.RenderWhere(queryFragment));
        }
        foreach (WhereIn<Query> queryFragment in this.QueryWhereIn) {
            if (rendered.Any()) {
                rendered.Add(queryFragment.Operator.GetDescription());
            }
            rendered.Add(this.RenderWhereIn(queryFragment));
        }

        return (rendered.Count > 0 ? "WHERE " + string.Join(' ', rendered) : "");
    }

    /// <inheritdoc/>
    protected override string _RenderGroupSentence() {
        List<string> rendered = new List<string>();

        foreach (GroupBy queryFragment in this.QueryGroup) {
            rendered.Add(this.RenderGroupBy(queryFragment));
        }

        return (rendered.Count > 0 ? "GROUP BY " + string.Join(',', rendered) : "");
    }

    /// <inheritdoc/>
    protected override string _RenderHavingSentence() {
        List<string> rendered = new List<string>();

        foreach (Where<Query> queryFragment in this.QueryHaving) {
            rendered.Add(this.RenderWhere(queryFragment));
        }

        return (rendered.Count > 0 ? "HAVING " + string.Join(',', rendered) : "");
    }

    /// <inheritdoc/>
    protected override string _RenderOrderSentence() {
        List<string> rendered = new List<string>();

        if (this.QueryOrder != null) {
            foreach (OrderBy queryOrderItem in this.QueryOrder) {
                List<string> renderedSubset = new List<string>();

                renderedSubset.Add(queryOrderItem.Field.Render());

                if (queryOrderItem.Direction != OrderDirection.NONE) {
                    renderedSubset.Add(queryOrderItem.Direction.GetDescription());
                }

                rendered.Add(string.Join(' ', renderedSubset));
            }
        }

        return (rendered.Count > 0 ? "ORDER BY " + string.Join(',', rendered) : "");
    }

    /// <inheritdoc/>
    protected override string _RenderLimitSentence() {
        List<string> rendered = new List<string>();
        if (this.QueryLimit != null) {
            if (this.QueryLimit.Count > 0) {
                rendered.Add(this.QueryLimit.Count.ToString());
            }
            if (this.QueryLimit.Offset >= 0) {
                rendered.Add($" OFFSET {this.QueryLimit.Offset.ToString()}");
            }
        }

        return (rendered.Count > 0 ? "LIMIT " + string.Join(' ', rendered) : "");
    }

    /// <inheritdoc/>
    protected override string _RenderDeleteSentence() {
        From<Query> from = this.QueryFrom.FirstOrDefault();

        if (from != null) {
            return $"DELETE FROM {from.Table}{(!string.IsNullOrWhiteSpace(from.TableAlias) ? $" AS {from.TableAlias}" : "")}";
        }

        return string.Empty;
    }

    /// <inheritdoc/>
    protected override string _RenderUpdateSentence() { 
        From<Query> from = this.QueryFrom.FirstOrDefault();

        if (from != null) {
            return $"UPDATE {this.RenderFrom(from)}";
        }

        return string.Empty;
    }

    /// <inheritdoc/>
    protected override string _RenderSetSentence() {
        List<string> rendered = new List<string>();

        if (this.QueryOrder != null) {
            foreach (Where<Query> querySetItem in this.QuerySet) {
                querySetItem.Comparer = WhereComparer.EQUALS;

                rendered.Add(this.RenderWhere(querySetItem));
            }
        }

        return (rendered.Count > 0 ? "SET " + string.Join(',', rendered) : "");
    }

    /// <inheritdoc/>
    protected override string _RenderInsertIntoSentence() {
        if (this.QueryCopyInto != null && this.QueryFrom.Any()) {
            return $"COPY {this.RenderFrom(this.QueryCopyInto)} FROM {this.RenderFrom(this.QueryFrom.FirstOrDefault())}";
        }

        From<Query> from = this.QueryFrom.FirstOrDefault();

        if (from != null) {
            return $"INSERT INTO {Query.FieldDelimiter}{from.Table}{Query.FieldDelimiter} ({string.Join(',', this.QueryColumns.Select(column => $"{Query.FieldDelimiter}{column}{Query.FieldDelimiter}"))})";
        }

        return string.Empty;
    }

    /// <inheritdoc/>
    protected override string _RenderInsertValuesSentence() {
        List<string> rendered = new List<string>();

        if (this.QueryValues != null) {
            foreach (Dictionary<string, dynamic> queryValue in QueryValues) {
                List<string> toRender = new List<string>();

                // In order to get a valid query, insert the values in the same column order
                foreach (string queryColumn in this.QueryColumns) {
                    if (queryValue.ContainsKey(queryColumn) && queryValue[queryColumn] != null) {
                        if (queryValue[queryColumn] != null && queryValue[queryColumn] is DuckDBParameter) {
                            string preparedQuerylabel = this.GetNextPreparedQueryValueLabel();
                            (queryValue[queryColumn] as DuckDBParameter).ParameterName = preparedQuerylabel;

                            toRender.Add(this.PrepareQueryValue(queryValue[queryColumn], false));
                            continue;
                        }
                    
                        if (queryValue[queryColumn] != null && queryValue[queryColumn] is Enum) {
                            toRender.Add(this.PrepareQueryValue(queryValue[queryColumn], false));
                        }
                        else {
                            toRender.Add(this.PrepareQueryValue(queryValue[queryColumn], true));
                        }
                    }
                    else {
                        toRender.Add("NULL");
                    }
                }

                rendered.Add($"({string.Join(",", toRender)})");
            }
        }

        return (rendered.Count > 0 ? 
            $"VALUES {string.Join(',', rendered)}" + 
            ((!string.IsNullOrWhiteSpace(QueryReturning) && this.QueryValues.Count == 1 ? $" RETURNING {QueryReturning}" : "")) 
            :
            ""
        );
    }

    /// <inheritdoc/>
    protected override string _RenderInsertOnConflictSentence() {
        return string.Empty;
    }

    /// <inheritdoc/>
    protected override string _RenderCreateSentence<T>() {
        return this._RenderCreateSentence(typeof(T));
    }

    /// <inheritdoc/>
    protected override string _RenderCreateSentence(Type tableType) {
        Table typeTable = tableType.GetCustomAttribute<Table>();
        if (typeTable == null) {
            throw new InvalidOperationException("Missing [Table] attribute");
        }

        StringBuilder rendered = new StringBuilder();

        rendered.Append("CREATE ");
        if (typeTable.Temporary) {
            rendered.Append("TEMPORARY ");
        }

        rendered.Append("TABLE ");
        if (typeTable.IfNotExists) {
            rendered.Append("IF NOT EXISTS ");
        }
        rendered.Append($"{Query.FieldDelimiter}{typeTable.Name}{Query.FieldDelimiter} (");

        IEnumerable<string?> tableColumnDefinitions = this.__GetTableColumnDefinitions(tableType);
        IEnumerable<string?> tableKeyDefinitions    = this.__GetTableKeyDefinitions(tableType);

        rendered.Append(string.Join(",", tableColumnDefinitions.Concat(tableKeyDefinitions ?? Enumerable.Empty<string>())));
        rendered.Append(")");

        // Table options
        var tableOptions = tableType.GetCustomAttributes<TableOption>();
        foreach (TableOption tableOption in tableOptions) {
            rendered.Append($" {tableOption.Name}={tableOption.Value}");
        }

        return rendered.ToString();
    }

    /// <summary>
    /// Gets the column definitions for a table type.
    /// </summary>
    /// <param name="tableType">The table type.</param>
    /// <returns>The column definitions as strings.</returns>
    private IEnumerable<string?> __GetTableColumnDefinitions(Type tableType) {
        PropertyInfo[] tableProperties = tableType.GetProperties();
        FieldInfo   [] tableFields     = tableType.GetFields();

        return tableProperties.Select(tableProperty => {
            return this.__GetColumnDefinition(tableProperty, tableProperty.GetCustomAttribute<Column>());
        }).Where(renderedColumn => renderedColumn != null);
    }

    /// <summary>
    /// Generates a collection of SQL key constraint definitions for the specified table type.
    /// </summary>
    /// <remarks>This method inspects the custom attributes applied to the specified table type to generate SQL key
    /// constraint  definitions. Supported key attributes include: <list type="bullet"> <item><description><see
    /// cref="Key"/>: Defines a general key constraint.</description></item> <item><description><see cref="PrimaryKey"/>:
    /// Defines a primary key constraint.</description></item> <item><description><see cref="UniqueKey"/>: Defines a unique
    /// key constraint.</description></item> <item><description><see cref="ForeignKey"/>: Defines a foreign key constraint,
    /// including references to another table.</description></item> </list> The generated SQL definitions include details
    /// such as constraint names, column names, and optional index types  (e.g., BTREE, HASH). For foreign keys, additional
    /// clauses like <c>ON DELETE</c> and <c>ON UPDATE</c> are included  if specified.</remarks>
    /// <param name="tableType">The type representing the table for which key constraint definitions are generated.  This type must have attributes
    /// defining keys, such as <see cref="Key"/>, <see cref="PrimaryKey"/>,  <see cref="UniqueKey"/>, or <see
    /// cref="ForeignKey"/>.</param>
    /// <returns>An <see cref="IEnumerable{T}"/> of strings, where each string represents a SQL key constraint definition  (e.g.,
    /// primary key, unique key, foreign key) for the specified table type. If no key attributes are found,  the collection
    /// will be empty.</returns>
    private IEnumerable<string?> __GetTableKeyDefinitions(Type tableType) {
        List<string> definitions = new List<string>();

        foreach (UniqueKey uKey in tableType.GetCustomAttributes<UniqueKey>()) {
            definitions.Add(
                $"CONSTRAINT {Query.FieldDelimiter}uk_{uKey.Name}{Query.FieldDelimiter} UNIQUE " +
                $"({string.Join(", ", uKey.Columns.Select(column => $"{Query.FieldDelimiter}{ReflectionCache.GetColumnName(tableType, column)}{Query.FieldDelimiter}"))})"
            );
        }
        foreach (ForeignKey fKey in tableType.GetCustomAttributes<ForeignKey>()) {
            definitions.Add(
                $"CONSTRAINT {Query.FieldDelimiter}fk_{fKey.Name}{Query.FieldDelimiter} FOREIGN KEY " +
                $"({string.Join(", ", fKey.Columns.Select(column => $"{Query.FieldDelimiter}{column}{Query.FieldDelimiter}"))}) " + 
                $" REFERENCES {Query.FieldDelimiter}{fKey.ReferencedTable}{Query.FieldDelimiter}" +
                $"({string.Join(", ", fKey.ReferencedColumns.Select(column => $"{Query.FieldDelimiter}{column}{Query.FieldDelimiter}"))})" + 
                $"{(!string.IsNullOrWhiteSpace(fKey.OnDelete) ? $" ON DELETE {fKey.OnDelete}" : "")}" + 
                $"{(!string.IsNullOrWhiteSpace(fKey.OnUpdate) ? $" ON UPDATE {fKey.OnUpdate}" : "")}" 
            );
        }

        return definitions;
    }

    /// <summary>
    /// Generates the SQL column definition string for a given property and table column.
    /// </summary>
    /// <remarks>The generated column definition includes the column name, data type, length, precision,
    /// constraints (e.g., NOT NULL, UNIQUE), default values, comments, and other attributes based on the provided
    /// <paramref name="tableColumn"/> metadata. Special handling is applied for nullable types, enums, and specific
    /// data types such as VARCHAR and CHAR.</remarks>
    /// <param name="property">The <see cref="PropertyInfo"/> representing the property to map to the column.</param>
    /// <param name="tableColumn">The <see cref="Column"/> object containing metadata about the table column.</param>
    /// <returns>A string representing the SQL column definition, or <see langword="null"/> if <paramref name="tableColumn"/> is
    /// <see langword="null"/>.</returns>
    private string? __GetColumnDefinition(PropertyInfo property, Column tableColumn) {
        if (tableColumn == null) {
            return null;
        }

        Type columnType = property.PropertyType;
        if (Nullable.GetUnderlyingType(columnType) != null) {
            columnType = Nullable.GetUnderlyingType(columnType);
        }

        string columnDataTypeString = tableColumn.DataTypeString ?? this.GetColumnDataTypeString(tableColumn.DataType);

        StringBuilder columnBuilder = new StringBuilder($"{Query.FieldDelimiter}{tableColumn.Name}{Query.FieldDelimiter} {columnDataTypeString}");
        if (tableColumn.Length > 0)
            columnBuilder.Append($" ({tableColumn.Length}{(tableColumn.Precision > 0 ? $",{tableColumn.Precision}" : "")})");

        if (tableColumn.Unique)
            columnBuilder.Append(" UNIQUE");
        if (tableColumn.PrimaryKey && string.IsNullOrWhiteSpace(tableColumn.Default))
            columnBuilder.Append(" PRIMARY KEY");
        if (tableColumn.NotNull && !tableColumn.PrimaryKey) 
            columnBuilder.Append(" NOT NULL");
        if (tableColumn.Default != null)
            columnBuilder.Append($" DEFAULT {tableColumn.Default}");
        if (tableColumn.Check != null)
            columnBuilder.Append($" CHECK ({tableColumn.Check})");
        if (tableColumn.Comment != null)
            columnBuilder.Append($" COMMENT '{tableColumn.Comment}'");
        return columnBuilder.ToString();
    }

    /// <inheritdoc/>
    protected override string _RenderSelectExtraSentence() {
        return string.Empty;
    }
    #endregion

    #endregion

    #region Helper functions
    /// <summary>
    /// Converts the specified value into a string representation suitable for use in a "WHERE" clause, optionally
    /// applying delimiters for escaping.
    /// </summary>
    /// <param name="value">The value to be rendered. Can be a string, <see cref="DateTime"/>, <see cref="Enum"/>, or other types.</param>
    /// <param name="escape"><see langword="true"/> to apply delimiters around the value for escaping;  otherwise, <see langword="false"/>.</param>
    /// <returns>A string representation of the value, with delimiters applied if <paramref name="escape"/> is <see
    /// langword="true"/>  and the value is a string or <see cref="DateTime"/>. For <see cref="Enum"/> values, the
    /// description is returned.</returns>
    private string __RenderWhereValue(dynamic value, bool escape) {
        if (value is string
            ||
            value is DateTime
        ) {
            if (escape) {
                return $"{ValueDelimiter}{value}{ValueDelimiter}";
            }
        }
        if (value is Enum) {
            return $"{ValueDelimiter}{((Enum)value).GetDescription()}{ValueDelimiter}";
        }

        return value.ToString();
    }

    /// <summary>
    /// Converts a nullable <see cref="ColumnDataType"/> value to its corresponding DuckDB data type string.
    /// </summary>
    /// <remarks>This method maps logical column data types to their DuckDB equivalents based on common usage
    /// patterns. For instance, <see cref="ColumnDataType.Int32"/> maps to "INTEGER", while <see
    /// cref="ColumnDataType.Text"/> maps to "TEXT".</remarks>
    /// <param name="type">The nullable <see cref="ColumnDataType"/> to convert. Represents the logical data type of a database column.</param>
    /// <returns>A string representing the DuckDB data type that corresponds to the specified <paramref name="type"/>. For
    /// example, "INTEGER" for numeric types, "TEXT" for textual types, and "BLOB" for binary data.</returns>
    /// <exception cref="NotSupportedException">Thrown if the specified <paramref name="type"/> is not supported by DuckDB or is <c>null</c>.</exception>
    public string GetColumnDataTypeString(ColumnDataType? type) {
        return type switch {
            ColumnDataType.Boolean    => "BOOLEAN",
            ColumnDataType.Int16      => "SMALLINT",
            ColumnDataType.Int        => "INTEGER",
            ColumnDataType.Int32      => "INTEGER",
            ColumnDataType.Int64      => "BIGINT",
            ColumnDataType.UInt16     => "USMALLINT",
            ColumnDataType.UInt32     => "UINTEGER",
            ColumnDataType.UInt       => "UINTEGER",
            ColumnDataType.UInt64     => "UBIGINT",
            ColumnDataType.Decimal    => "DECIMAL",
            ColumnDataType.Float      => "FLOAT",
            ColumnDataType.Double     => "DOUBLE",
            ColumnDataType.TinyText   => "TEXT",
            ColumnDataType.Text       => "TEXT",
            ColumnDataType.MediumText => "TEXT",
            ColumnDataType.LongText   => "TEXT",
            ColumnDataType.Char       => "VARCHAR",
            ColumnDataType.Varchar    => "VARCHAR",
            ColumnDataType.Enum       => "ENUM",
            ColumnDataType.Date       => "TIMESTAMP",
            ColumnDataType.DateTime   => "TIMESTAMP",
            ColumnDataType.Time       => "TIME",
            ColumnDataType.Timestamp  => "TIMESTAMPTZ",
            ColumnDataType.Binary     => "BLOB",
            ColumnDataType.Guid       => "UUID",
            ColumnDataType.Json       => "VARCHAR",
            ColumnDataType.Xml        => "VARCHAR",

            _ => throw new NotSupportedException($"DuckDB does not support {type}")
        };
    }

    /// <summary>
    /// Maps a <see cref="ColumnDataType"/> to its corresponding <see cref="DuckDBDbType"/>.
    /// </summary>
    /// <remarks>This method provides a mapping between application-specific column data types and DuckDB
    /// data types. If the provided <paramref name="type"/> does not have  a defined mapping, the method returns 
    /// <see langword="null"/>.</remarks>
    /// <param name="type">The <see cref="ColumnDataType"/> to be mapped.</param>
    /// <returns>The corresponding <see cref="DuckDBDbType"/> for the specified <paramref name="type"/>,  or <see langword="null"/>
    /// if the mapping is not defined.</returns>
    public DuckDBType? GetDuckDBDataType(ColumnDataType type) {
        return type switch {
            ColumnDataType.Boolean   => DuckDBType.Boolean,
            ColumnDataType.Int16     => DuckDBType.SmallInt,
            ColumnDataType.Int       => DuckDBType.Integer,
            ColumnDataType.Int32     => DuckDBType.Integer,
            ColumnDataType.Int64     => DuckDBType.BigInt,
            ColumnDataType.UInt16    => DuckDBType.UnsignedSmallInt,
            ColumnDataType.UInt32    => DuckDBType.UnsignedInteger,
            ColumnDataType.UInt      => DuckDBType.UnsignedInteger,
            ColumnDataType.UInt64    => DuckDBType.UnsignedBigInt,
            ColumnDataType.Decimal   => DuckDBType.Decimal,
            ColumnDataType.Float     => DuckDBType.Float,
            ColumnDataType.Double    => DuckDBType.Double,
            ColumnDataType.TinyText  => DuckDBType.Varchar,
            ColumnDataType.Text      => DuckDBType.Varchar,
            ColumnDataType.MediumText=> DuckDBType.Varchar,
            ColumnDataType.LongText  => DuckDBType.Varchar,
            ColumnDataType.Char      => DuckDBType.Varchar,
            ColumnDataType.Varchar   => DuckDBType.Varchar,
            ColumnDataType.Enum      => DuckDBType.Enum,
            ColumnDataType.Date      => DuckDBType.Date,
            ColumnDataType.DateTime  => DuckDBType.Timestamp,
            ColumnDataType.Time      => DuckDBType.Time,
            ColumnDataType.Timestamp => DuckDBType.TimestampTz,
            ColumnDataType.Binary    => DuckDBType.Blob,
            ColumnDataType.Guid      => DuckDBType.Uuid,
            ColumnDataType.Json      => DuckDBType.Varchar,
            ColumnDataType.Xml       => DuckDBType.Varchar,

            _ => null
        };
    }

    public DbType GetSystemDBType(Type? type) {
        return true switch {
            true when type == typeof(bool)     => DbType.Boolean,
            true when type == typeof(sbyte)    => DbType.SByte,
            true when type == typeof(short)    => DbType.Int16,
            true when type == typeof(int)      => DbType.Int32,
            true when type == typeof(long)     => DbType.Int64,
            true when type == typeof(float)    => DbType.Single,
            true when type == typeof(double)   => DbType.Double,
            true when type == typeof(string)   => DbType.String,
            true when type == typeof(Guid)     => DbType.Guid,
            true when type == typeof(decimal)  => DbType.Currency,
            true when type == typeof(byte)     => DbType.Byte,
            true when type == typeof(ushort)   => DbType.UInt16,
            true when type == typeof(uint)     => DbType.UInt32,
            true when type == typeof(ulong)    => DbType.UInt64,
            true when type == typeof(byte)     => DbType.Binary,
            true when type == typeof(DateTime) => DbType.DateTime,

            _ => DbType.String
        };
    }
    #endregion
}
