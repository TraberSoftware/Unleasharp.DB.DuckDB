using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using Unleasharp.DB.Base.QueryBuilding;
using Unleasharp.ExtensionMethods;

namespace Unleasharp.DB.DuckDB;

public static class QueryRendererExtensions {
    /// <summary>
    /// Renders the current <see cref="FieldSelector"/> instance into a string representation suitable for use in a
    /// query, combining the table and field names with proper formatting.
    /// </summary>
    /// <remarks>The method ensures proper formatting of the table and field names based on the <see
    /// cref="FieldSelector"/> properties. If escaping is enabled, the table and field names are enclosed in the
    /// delimiter defined by <see cref="Query.FieldDelimiter"/>.</remarks>
    /// <param name="fragment">The <see cref="FieldSelector"/> instance containing the table and field information to render.</param>
    /// <returns>A string representation of the table and field names, separated by a period ('.').  If the <see
    /// cref="FieldSelector"/> specifies escaping, the table and field names are enclosed in delimiters. Returns an
    /// empty string if both the table and field are null, empty, or whitespace.</returns>
    public static string Render(this FieldSelector fragment) {
        List<string> toRender = new List<string>();

        if (!string.IsNullOrWhiteSpace(fragment.Table)) {
            if (fragment.Escape) {
                toRender.Add(Query.FieldDelimiter + fragment.Table + Query.FieldDelimiter);
            }
            else {
                toRender.Add(fragment.Table);
            }
        }

        if (!string.IsNullOrWhiteSpace(fragment.Field)) {
            if (fragment.Escape && fragment.Field.Trim() != "*") {
                toRender.Add(Query.FieldDelimiter + fragment.Field + Query.FieldDelimiter);
            }
            else {
                toRender.Add(fragment.Field);
            }
        }

        return String.Join('.', toRender);
    }
}
