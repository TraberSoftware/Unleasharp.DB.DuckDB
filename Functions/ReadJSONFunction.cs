using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Unleasharp.DB.DuckDB.Functions;

/// <summary>
/// Enumeration of supported JSON input formats.
/// </summary>
public enum JSONFormat {
    Auto,
    Unstructured,
    NewlineDelimited,
    Array
}

/// <summary>
/// Represents a configuration for reading JSON files with customizable options.
/// </summary>
/// <remarks>
/// The <see cref="ReadJSONFunction"/> class provides a flexible way to configure and execute JSON file reading operations.
/// It supports a wide range of options, including format selection, auto schema detection, error handling, and more.
/// This class is typically used to construct a query for reading JSON files in a structured format.
/// </remarks>
public class ReadJSONFunction {
    #region Shared Parameters

    /// <summary>
    /// Compression method of the file(s). Options are none, gzip, zstd, auto_detect. Default: auto_detect
    /// </summary>
    public string? Compression { get; set; }

    /// <summary>
    /// Add path of the containing file as a column. Default: false
    /// </summary>
    public bool? Filename { get; set; }

    /// <summary>
    /// Format of the JSON input: auto, unstructured, newline_delimited, array. Default: array
    /// </summary>
    public JSONFormat? Format { get; set; }

    /// <summary>
    /// Interpret the path as Hive-partitioned. Default: auto-detected
    /// </summary>
    public bool? HivePartitioning { get; set; }

    /// <summary>
    /// Ignore parse errors (only when format = newline_delimited). Default: false
    /// </summary>
    public bool? IgnoreErrors { get; set; }

    /// <summary>
    /// Maximum number of JSON files sampled for auto-detection. Default: 32
    /// </summary>
    public long? MaximumSampleFiles { get; set; }

    /// <summary>
    /// Maximum size of a JSON object (bytes). Default: 16777216
    /// </summary>
    public long? MaximumObjectSize { get; set; }

    /// <summary>
    /// Whether the schema of multiple files should be unified. Default: false
    /// </summary>
    public bool? UnionByName { get; set; }

    #endregion

    #region AsFunction Parameters

    /// <summary>
    /// Auto-detect names and data types of keys. Default: true
    /// </summary>
    public bool? AutoDetect { get; set; }

    /// <summary>
    /// Explicitly define key names and value types as a struct. Default: empty
    /// </summary>
    public Dictionary<string, string>? Columns { get; set; }

    /// <summary>
    /// Date format when parsing dates. Default: iso
    /// </summary>
    public string? DateFormat { get; set; }

    /// <summary>
    /// Maximum nesting depth for type detection. -1 = fully detect. Default: -1
    /// </summary>
    public long? MaximumDepth { get; set; }

    /// <summary>
    /// Whether to treat JSON as records (auto, true, false). Default: auto
    /// </summary>
    public string? Records { get; set; }

    /// <summary>
    /// Number of sample objects for auto detection. -1 = full scan. Default: 20480
    /// </summary>
    public long? SampleSize { get; set; }

    /// <summary>
    /// Timestamp format when parsing. Default: iso
    /// </summary>
    public string? TimestampFormat { get; set; }

    /// <summary>
    /// Threshold for inferring MAP vs STRUCT. -1 disables. Default: 200
    /// </summary>
    public long? MapInferenceThreshold { get; set; }

    /// <summary>
    /// Threshold for field appearance frequency before using MAP type. Default: 0.1
    /// </summary>
    public double? FieldAppearanceThreshold { get; set; }

    #endregion

    #region AsCopyFrom Parameters

    /// <summary>
    /// Convert integer-like strings to numbers. Default: false
    /// </summary>
    public bool? ConvertStringsToIntegers { get; set; }

    #endregion

    /// <summary>
    /// Path to the CSV file.
    /// </summary>
    private string __Path;

    /// <summary>
    /// Path to the CSV files.
    /// </summary>
    private List<string>? __Paths = new List<string>();

    /// <summary>
    /// Path to the CSV files. In case multiple paths have not been set, it will
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
    /// Path to the CSV file. Required. In case multiple paths have been set, it will
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
            throw new InvalidOperationException("FileName must be set for ReadJSON");
        }

        var options = new List<string>();

        #region Helpers
        void AddBool(string name, bool? value) {
            if (value != null)
                options.Add($"{name} = {value.Value.ToString().ToLowerInvariant()}");
        }
        void AddString(string name, string? value) {
            if (value != null)
                options.Add($"{name} = '{value}'");
        }
        void AddLong(string name, long? value) {
            if (value != null)
                options.Add($"{name} = {value}");
        }
        void AddDouble(string name, double? value) {
            if (value != null)
                options.Add($"{name} = {value.Value.ToString(CultureInfo.InvariantCulture)}");
        }
        void AddDict(string name, Dictionary<string, string>? dict) {
            if (dict != null && dict.Count > 0) {
                var parts = new List<string>();
                foreach (var kv in dict)
                    parts.Add($"'{kv.Key}': '{kv.Value}'");
                options.Add($"{name} = {{{string.Join(", ", parts)}}}");
            }
        }
        #endregion

        #region Options
        if (Format != null) {
            options.Add($"format = '{Format.ToString()!.ToLowerInvariant()}'");
        }
        AddString("compression",                Compression);
        AddBool  ("filename",                   Filename);
        AddBool  ("hive_partitioning",          HivePartitioning);
        AddBool  ("ignore_errors",              IgnoreErrors);
        AddLong  ("maximum_sample_files",       MaximumSampleFiles);
        AddLong  ("maximum_object_size",        MaximumObjectSize);
        AddBool  ("union_by_name",              UnionByName);
        AddBool  ("auto_detect",                AutoDetect);
        AddDict  ("columns",                    Columns);
        AddString("dateformat",                 DateFormat);
        AddLong  ("maximum_depth",              MaximumDepth);
        AddString("records",                    Records);
        AddLong  ("sample_size",                SampleSize);
        AddString("timestampformat",            TimestampFormat);
        AddLong  ("map_inference_threshold",    MapInferenceThreshold);
        AddDouble("field_appearance_threshold", FieldAppearanceThreshold);
        #endregion

        var files = Paths.Count == 1
            ? $"'{Paths[0]}'"
            : $"[{string.Join(", ", Paths.ConvertAll(f => $"'{f}'"))}]";

        return $"read_json({files}{(options.Count > 0 ? $", {string.Join(", ", options)}" : "")})";
    }

    public string AsCopyFrom() {
        if (Paths == null || Paths.Count == 0) {
            throw new InvalidOperationException("FileName must be set for COPY");
        }

        var options = new List<string>();

        #region Helpers
        void AddBool(string name, bool? value) {
            if (value == true) {
                options.Add(name.ToUpperInvariant());
            }
            else if (value == false) {
                options.Add($"{name.ToUpperInvariant()} FALSE");
            }
        }
        void AddString(string name, string? value) {
            if (value != null)
                options.Add($"{name.ToUpperInvariant()} '{value}'");
        }
        void AddLong(string name, long? value) {
            if (value != null)
                options.Add($"{name.ToUpperInvariant()} {value}");
        }
        void AddDict(string name, Dictionary<string, string>? dict) {
            if (dict != null && dict.Count > 0) {
                var parts = new List<string>();
                foreach (var kv in dict)
                    parts.Add($"'{kv.Key}': '{kv.Value}'");
                options.Add($"{name.ToUpperInvariant()} {{{string.Join(", ", parts)}}}");
            }
        }
        #endregion

        #region Options
        if (Format != null) {
            AddBool("array", Format == JSONFormat.Array);
        }
        AddBool  ("auto_detect",                 AutoDetect);
        AddString("compression",                 Compression);
        AddBool  ("convert_strings_to_integers", ConvertStringsToIntegers);
        AddString("dateformat",                  DateFormat);
        AddBool  ("filename",                    Filename);
        AddBool  ("hive_partitioning",           HivePartitioning);
        AddBool  ("ignore_errors",               IgnoreErrors);
        AddLong  ("maximum_depth",               MaximumDepth);
        AddLong  ("maximum_object_size",         MaximumObjectSize);
        AddString("records",                     Records);
        AddLong  ("sample_size",                 SampleSize);
        AddString("timestampformat",             TimestampFormat);
        AddBool  ("union_by_name",               UnionByName);
        #endregion

        var files = Paths.Count == 1
            ? $"'{Paths[0]}'"
            : $"[{string.Join(", ", Paths.ConvertAll(f => $"'{f}'"))}]";

        return $"{files}{(options.Count > 0 ? $" ({string.Join(", ", options)})" : "")};";
    }
}
