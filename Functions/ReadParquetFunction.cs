using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Unleasharp.DB.DuckDB.Functions;

/// <summary>
/// Represents a configuration for reading Parquet files with various customizable options.
/// </summary>
/// <remarks>
/// The <see cref="ReadParquetFunction"/> class provides a flexible way to configure and execute Parquet file reading operations.
/// It supports a range of options, including file compression, Hive partitioning, union by name, and more.
/// This class is typically used to construct a query for reading Parquet files in a structured format.
/// </remarks>
public class ReadParquetFunction {
    /// <summary>
    /// Whether file names should be included as a column. Default: false
    /// </summary>
    public bool? Filename { get; set; }

    /// <summary>
    /// Treat hive-partitioned Parquet files as partitioned tables. Default: auto
    /// </summary>
    public bool? HivePartitioning { get; set; }

    /// <summary>
    /// Union schemas by column name across Parquet files. Default: false
    /// </summary>
    public bool? UnionByName { get; set; }

    /// <summary>
    /// Enable reading compressed Parquet files. Default: auto
    /// </summary>
    public string? Compression { get; set; }

    /// <summary>
    /// Use parallel Parquet reader. Default: true
    /// </summary>
    public bool? Parallel { get; set; }

    /// <summary>
    /// Normalize column names. Default: false
    /// </summary>
    public bool? NormalizeNames { get; set; }

    /// <summary>
    /// Allows the user to select a subset of row groups. Default: all
    /// </summary>
    public List<long>? RowGroupIds { get; set; }

    /// <summary>
    /// Allows the user to select a subset of columns. Default: all
    /// </summary>
    public List<string>? Columns { get; set; }

    /// <summary>
    /// Ignore parsing errors. Default: false
    /// </summary>
    public bool? IgnoreErrors { get; set; }

    /// <summary>
    /// Path to the Parquet file.
    /// </summary>
    private string __Path;

    /// <summary>
    /// Path to the Parquet files.
    /// </summary>
    private List<string>? __Paths = new List<string>();

    /// <summary>
    /// Path to the Parquet files. In case multiple paths have not been set, it will
    /// return a list with the single Path provided.
    /// </summary>
    public List<string>? Paths {
        get {
            if (__Paths != null && __Paths.Count > 0) {
                return __Paths;
            }

            if (!string.IsNullOrWhiteSpace(__Path)) {
                return new List<string> { __Path };
            }

            return default;
        }
        set {
            __Paths = value;
        }
    }

    /// <summary>
    /// Path to the Parquet file. Required. In case multiple paths have been set, it will
    /// return the first item of the Paths property.
    /// </summary>
    public string Path {
        get {
            if (__Paths != null && __Paths.Count > 0) {
                return __Paths.FirstOrDefault();
            }

            if (!string.IsNullOrWhiteSpace(__Path)) {
                return __Path;
            }

            return __Path;
        }
        set {
            __Path = value;
        }
    }

    public string AsFunction() {
        if (Paths == null || Paths.Count == 0) {
            throw new InvalidOperationException("Path(s) must be set for ReadParquet");
        }

        var options = new List<string>();

        #region Internal helpers
        void AddBool(string name, bool? value) {
            if (value != null)
                options.Add($"{name} = {value.Value.ToString().ToLowerInvariant()}");
        }
        void AddString(string name, string? value) {
            if (value != null)
                options.Add($"{name} = '{value}'");
        }
        void AddList(string name, IEnumerable<string>? values) {
            if (values != null && values.Any())
                options.Add($"{name} = ['{string.Join("', '", values)}']");
        }
        void AddLongList(string name, IEnumerable<long>? values) {
            if (values != null && values.Any())
                options.Add($"{name} = [{string.Join(", ", values)}]");
        }
        #endregion

        #region Options rendering
        AddBool    ("filename",          Filename);
        AddBool    ("hive_partitioning", HivePartitioning);
        AddBool    ("union_by_name",     UnionByName);
        AddString  ("compression",       Compression);
        AddBool    ("parallel",          Parallel);
        AddBool    ("normalize_names",   NormalizeNames);
        AddLongList("row_group_ids",     RowGroupIds);
        AddList    ("columns",           Columns);
        AddBool    ("ignore_errors",     IgnoreErrors);
        #endregion

        var files = Paths.Count == 1
            ? $"'{Paths[0]}'"
            : $"[{string.Join(", ", Paths.ConvertAll(f => $"'{f}'"))}]";

        return $"read_parquet({files}{(options.Count > 0 ? $", {string.Join(", ", options)}" : "")})";
    }

    public string AsCopyFrom() {
        if (Paths == null || Paths.Count == 0) {
            throw new InvalidOperationException("Path(s) must be set for COPY");
        }

        var options = new List<string>();

        #region Internal helpers
        void AddBool(string name, bool? value) {
            if (value == true) {
                options.Add(name.ToUpperInvariant());
            }
            else if (value == false) {
                options.Add($"{name.ToUpperInvariant()} FALSE");
            }
        }
        void AddString(string name, string? value) {
            if (value != null) {
                options.Add($"{name.ToUpperInvariant()} '{value}'");
            }
        }
        void AddList(string name, IEnumerable<string>? values) {
            if (values != null && values.Any()) {
                options.Add($"{name.ToUpperInvariant()} ['{string.Join("', '", values)}']");
            }
        }
        void AddLongList(string name, IEnumerable<long>? values) {
            if (values != null && values.Any()) {
                options.Add($"{name.ToUpperInvariant()} [{string.Join(", ", values)}]");
            }
        }
        #endregion

        #region Options rendering
        AddBool    ("filename",          Filename);
        AddBool    ("hive_partitioning", HivePartitioning);
        AddBool    ("union_by_name",     UnionByName);
        AddString  ("compression",       Compression);
        AddBool    ("parallel",          Parallel);
        AddBool    ("normalize_names",   NormalizeNames);
        AddLongList("row_group_ids",     RowGroupIds);
        AddBool    ("ignore_errors",     IgnoreErrors);
        #endregion

        var files = Paths.Count == 1
            ? $"'{Paths[0]}'"
            : $"[{string.Join(", ", Paths.ConvertAll(f => $"'{f}'"))}]";

        return $"{files}{(options.Count > 0 ? $" ({string.Join(", ", options)})" : "")};";
    }
}
