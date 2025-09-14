using System;
using System.Collections.Generic;
using System.Text;

namespace Unleasharp.DB.DuckDB.Functions;

/// <summary>
/// Represents a configuration for reading CSV files with various customizable options.
/// </summary>
/// <remarks>The <see cref="ReadCSVFunction"/> class provides a flexible way to configure and execute CSV file reading operations.
/// It supports a wide range of options, including column type detection, delimiter customization, error handling, and more.
/// This class is typically used to construct a query for reading CSV files in a structured format.</remarks>
public class ReadCSVFunction {
    /// <summary>
    /// Skip type detection and assume all columns are of type VARCHAR. Default: false
    /// </summary>
    public bool? AllVarchar { get; set; }

    /// <summary>
    /// Allow the conversion of quoted values to NULL values. Default: true
    /// </summary>
    public bool? AllowQuotedNulls { get; set; }

    /// <summary>
    /// Automatically detect CSV parameters. Default: true
    /// </summary>
    public bool? AutoDetect { get; set; }

    /// <summary>
    /// Types that the sniffer uses when detecting column types. Default: ['SQLNULL', 'BOOLEAN', 'BIGINT', 'DOUBLE', 'TIME', 'DATE', 'TIMESTAMP', 'VARCHAR']
    /// </summary>
    public List<string>? AutoTypeCandidates { get; set; }

    /// <summary>
    /// Size of the buffers used to read files, in bytes. Default: 16 * max_line_size
    /// </summary>
    public long? BufferSize { get; set; }

    /// <summary>
    /// Column names and types, as a struct. Default: empty struct
    /// </summary>
    public Dictionary<string, string>? Columns { get; set; }

    /// <summary>
    /// Character used to initiate comments. Default: empty
    /// </summary>
    public string? Comment { get; set; }

    /// <summary>
    /// Method used to compress CSV files. Default: auto
    /// </summary>
    public string? Compression { get; set; }

    /// <summary>
    /// Date format used when parsing dates. Default: empty
    /// </summary>
    public string? DateFormat { get; set; }

    /// <summary>
    /// Alias for dateformat; only in COPY statement. Default: empty
    /// </summary>
    public string? Date_Format { get; set; }

    /// <summary>
    /// Decimal separator for numbers. Default: "."
    /// </summary>
    public string? DecimalSeparator { get; set; }

    /// <summary>
    /// Delimiter character used to separate columns. Default: ","
    /// </summary>
    public string? Delim { get; set; }

    /// <summary>
    /// Alias for delim; only in COPY. Default: ","
    /// </summary>
    public string? Delimiter { get; set; }

    /// <summary>
    /// String used to escape the quote character. Default: "\"
    /// </summary>
    public string? Escape { get; set; }

    /// <summary>
    /// Encoding used by the CSV file. Default: utf-8
    /// </summary>
    public string? Encoding { get; set; }

    /// <summary>
    /// Add path of the containing file as a column. Default: false
    /// </summary>
    public bool? Filename { get; set; }

    /// <summary>
    /// Do not match values in specified columns against the NULL string. Default: empty list
    /// </summary>
    public List<string>? ForceNotNull { get; set; }

    /// <summary>
    /// First line contains column names. Default: false
    /// </summary>
    public bool? Header { get; set; }

    /// <summary>
    /// Interpret the path as Hive-partitioned. Default: auto-detected
    /// </summary>
    public bool? HivePartitioning { get; set; }

    /// <summary>
    /// Ignore parsing errors. Default: false
    /// </summary>
    public bool? IgnoreErrors { get; set; }

    /// <summary>
    /// Maximum line size, in bytes. Default: 2000000
    /// </summary>
    public long? MaxLineSize { get; set; }

    /// <summary>
    /// Column names, as a list. Default: empty list
    /// </summary>
    public List<string>? Names { get; set; }

    /// <summary>
    /// New line character(s). Default: empty
    /// </summary>
    public string? NewLine { get; set; }

    /// <summary>
    /// Normalize column names. Default: false
    /// </summary>
    public bool? NormalizeNames { get; set; }

    /// <summary>
    /// Pad missing columns with NULLs. Default: false
    /// </summary>
    public bool? NullPadding { get; set; }

    /// <summary>
    /// Strings that represent NULL values. Default: empty
    /// </summary>
    public object? NullStr { get; set; }

    /// <summary>
    /// Use the parallel CSV reader. Default: true
    /// </summary>
    public bool? Parallel { get; set; }

    /// <summary>
    /// String used to quote values. Default: "
    /// </summary>
    public string? Quote { get; set; }

    /// <summary>
    /// Temp table for faulty scans. Default: rejects_scans
    /// </summary>
    public string? RejectsScan { get; set; }

    /// <summary>
    /// Temp table for faulty lines. Default: reject_errors
    /// </summary>
    public string? RejectsTable { get; set; }

    /// <summary>
    /// Upper limit on faulty lines per file. Default: 0
    /// </summary>
    public long? RejectsLimit { get; set; }

    /// <summary>
    /// Number of sample lines for auto detection. Default: 20480
    /// </summary>
    public long? SampleSize { get; set; }

    /// <summary>
    /// Align columns by name across files. Default: false
    /// </summary>
    public bool? UnionByName { get; set; }

    /// <summary>
    /// Path to the file(s). Required.
    /// </summary>
    public string? Path { get; set; }

    public string AsFunction() {
        if (string.IsNullOrEmpty(Path)) {
            throw new InvalidOperationException("Path must be set for ReadCSV");
        }

        var options = new List<string>();

        #region Internal methods for rendering
        void AddBool(string name, bool? value) {
            if (value != null)
                options.Add($"{name} = {value.Value.ToString().ToLowerInvariant()}");
        }
        void AddString(string name, string? value) {
            if (value != null)
                options.Add($"{name} = '{value}'");
        }
        void AddList(string name, IEnumerable<string>? values) {
            if (values != null)
                options.Add($"{name} = ['{string.Join("', '", values)}']");
        }
        void AddDict(string name, Dictionary<string, string>? dict) {
            if (dict != null && dict.Count > 0) {
                var parts = new List<string>();
                foreach (var kv in dict)
                    parts.Add($"'{kv.Key}': '{kv.Value}'");
                options.Add($"{name} = {{{string.Join(", ", parts)}}}");
            }
        }
        void AddLong(string name, long? value) {
            if (value != null) {
                options.Add($"{name.ToUpperInvariant()} {value}");
            }
        }
        #endregion

        #region Options rendering
        // Only add when non-null
        AddBool  ("all_varchar",          AllVarchar);
        AddBool  ("allow_quoted_nulls",   AllowQuotedNulls);
        AddBool  ("auto_detect",          AutoDetect);
        AddList  ("auto_type_candidates", AutoTypeCandidates);
        AddLong  ("buffer_size",          BufferSize);
        AddDict  ("columns",              Columns);
        AddString("comment",              Comment);
        AddString("compression",          Compression);
        AddString("dateformat",           DateFormat);
        AddString("date_format",          Date_Format);
        AddString("decimal_separator",    DecimalSeparator);
        AddString("delim",                Delim);
        AddString("delimiter",            Delimiter);
        AddString("escape",               Escape);
        AddString("encoding",             Encoding);
        AddBool  ("filename",             Filename);
        AddList  ("force_not_null",       ForceNotNull);
        AddBool  ("header",               Header);
        AddBool  ("hive_partitioning",    HivePartitioning);
        AddBool  ("ignore_errors",        IgnoreErrors);
        AddLong  ("max_line_size",        MaxLineSize);
        AddList  ("names",                Names);
        AddString("new_line",             NewLine);
        AddBool  ("normalize_names",      NormalizeNames);
        AddBool  ("parallel",             Parallel);
        AddString("quote",                Quote);
        AddString("rejects_scan",         RejectsScan);
        AddString("rejects_table",        RejectsTable);
        AddLong  ("rejects_limit",        RejectsLimit);
        AddLong  ("sample_size",          SampleSize);
        AddBool  ("union_by_name",        UnionByName);
        AddBool  ("null_padding",         NullPadding);
        if (NullStr != null) {
            if (NullStr is List<string> list)
                AddList("nullstr", list);
            else
                AddString("nullstr", NullStr.ToString());
        }
        #endregion

        return $"read_csv('{Path}'{(options.Count > 0 ? $", {string.Join(", ", options)}" : "")})";
    }

    public string AsCopyFrom() {
        if (string.IsNullOrEmpty(Path)) {
            throw new InvalidOperationException("Path must be set for COPY");
        }

        var options = new List<string>();

        #region Internal methods for rendering
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
            if (values != null) {
                options.Add($"{name.ToUpperInvariant()} ['{string.Join("', '", values)}']");
            }
        }
        void AddDict(string name, Dictionary<string, string>? dict) {
            if (dict != null && dict.Count > 0) {
                var parts = new List<string>();
                foreach (var kv in dict) {
                    parts.Add($"'{kv.Key}': '{kv.Value}'");
                }
                options.Add($"{name.ToUpperInvariant()} {{{string.Join(", ", parts)}}}");
            }
        }
        void AddLong(string name, long? value) {
            if (value != null) {
                options.Add($"{name.ToUpperInvariant()} {value}");
            }
        }
        #endregion

        #region Options rendering
        // Only add when non-null
        AddBool  ("all_varchar",          AllVarchar);
        AddBool  ("allow_quoted_nulls",   AllowQuotedNulls);
        AddBool  ("auto_detect",          AutoDetect);
        AddList  ("auto_type_candidates", AutoTypeCandidates);
        AddLong  ("buffer_size",          BufferSize);
        //AddDict  ("columns",              Columns);
        AddString("comment",              Comment);
        AddString("compression",          Compression);
        AddString("dateformat",           DateFormat);
        AddString("date_format",          Date_Format);
        AddString("decimal_separator",    DecimalSeparator);
        AddString("delim",                Delim);
        AddString("delimiter",            Delimiter);
        AddString("escape",               Escape);
        AddString("encoding",             Encoding);
        AddBool  ("filename",             Filename);
        AddList  ("force_not_null",       ForceNotNull);
        AddBool  ("header",               Header);
        AddBool  ("hive_partitioning",    HivePartitioning);
        AddBool  ("ignore_errors",        IgnoreErrors);
        AddLong  ("max_line_size",        MaxLineSize);
        AddList  ("names",                Names);
        AddString("new_line",             NewLine);
        AddBool  ("normalize_names",      NormalizeNames);
        AddBool  ("parallel",             Parallel);
        AddString("quote",                Quote);
        AddString("rejects_scan",         RejectsScan);
        AddString("rejects_table",        RejectsTable);
        AddLong  ("rejects_limit",        RejectsLimit);
        AddLong  ("sample_size",          SampleSize);
        AddBool  ("union_by_name",        UnionByName);
        AddBool  ("null_padding",         NullPadding);
        if (NullStr != null) {
            if (NullStr is List<string> list)
                AddList("nullstr", list);
            else
                AddString("nullstr", NullStr.ToString());
        }
        #endregion

        return $"'{Path}'{(options.Count > 0 ? $" ({string.Join(", ", options)})" : "")};";
    }
}
