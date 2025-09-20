# 🦆 Unleasharp.DB.DuckDB

[![NuGet version (Unleasharp.DB.DuckDB)](https://img.shields.io/nuget/v/Unleasharp.DB.DuckDB.svg?style=flat-square)](https://www.nuget.org/packages/Unleasharp.DB.DuckDB/)
[![Github Pages](https://img.shields.io/badge/home-Github_Pages_-blue)](https://trabersoftware.github.io/Unleasharp.DB.Base)
[![Documentation](https://img.shields.io/badge/dev-Documentation-blue)](https://trabersoftware.github.io/Unleasharp.DB.Base/docs/)

[![Unleasharp.DB.DuckDB](https://socialify.git.ci/TraberSoftware/Unleasharp.DB.DuckDB/image?description=1&font=Inter&logo=https%3A%2F%2Fraw.githubusercontent.com%2FTraberSoftware%2FUnleasharp%2Frefs%2Fheads%2Fmain%2Fassets%2Flogo-small.png&name=1&owner=1&pattern=Circuit+Board&theme=Light)](https://github.com/TraberSoftware/Unleasharp.DB.DuckDB)

DuckDB implementation of Unleasharp.DB.Base. This repository provides a DuckDB-specific implementation that leverages the base abstraction layer for common database operations.

## 📦 Installation

Install the NuGet package using one of the following methods:

### Package Manager Console
```powershell
Install-Package Unleasharp.DB.DuckDB
```

### .NET CLI
```bash
dotnet add package Unleasharp.DB.DuckDB
```

### PackageReference (Manual)
```xml
<PackageReference Include="Unleasharp.DB.DuckDB" Version="1.8.1" />
```

## 🎯 Features

- **DuckDB-Specific Query Rendering**: Custom query building and rendering tailored for DuckDB
- **Connection Management**: Robust connection handling through ConnectorManager
- **Query Builder Integration**: Seamless integration with the base QueryBuilder
- **Schema Definition Support**: Full support for table and column attributes

## 🚀 Kickstart
```csharp
var db  = new ConnectorManager("Host=localhost;Database=unleasharp;Username=unleasharp;Password=unleasharp;")
var row = db.QueryBuilder().Build(query => query
    .From<ExampleTable>()
    .OrderBy<ExampleTable>(row => row.Id, OrderDirection.DESC)
    .Limit(1)
    .Select()
).FirstOrDefault<ExampleTable>();
```

## ⚠️ Disclaimer
DuckDB follows the PostgreSQL dialect. As of that, this project is an adaptation of Unleasharp.DB.PostgreSQL for DuckDB, it may not be ready for production use. 

Even so, specific features like Query.CreateSequence() have been implemented to ensure compatibility with basic table creation with auto-incremental columns, as well as data insertion and selection.


## 📖 Documentation Resources

- 📚 **[GitHub Pages Documentation](https://trabersoftware.github.io/Unleasharp.DB.Base/docs/)** - Complete documentation
- 🎯 **[Getting Started Guide](https://trabersoftware.github.io/Unleasharp.DB.Base/docs/getting-started/)** - Quick start guide
- 🦆 **[Unleasharp.DB DuckDB Documentation](https://trabersoftware.github.io/Unleasharp.DB.Base/docs/query-building/duckdb.html)** - Specific query builder documentation

## 📦 Dependencies

- [Unleasharp.DB.Base](https://github.com/TraberSoftware/Unleasharp.DB.Base) - Base abstraction layer
- [DuckDB.NET](https://github.com/Giorgi/DuckDB.NET) - Bindings and ADO.NET Provider for DuckDB

## 📋 Version Compatibility

This library targets .NET 6.0 and later versions. For specific version requirements, please check the package dependencies.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

*For more information about Unleasharp.DB.Base, visit: [Unleasharp.DB.Base](https://github.com/TraberSoftware/Unleasharp.DB.Base)*