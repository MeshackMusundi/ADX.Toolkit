# ADX.Toolkit
Kusto client wrapper for executing control commands and queries with retries using exponetial backoff.

# Usage
Install NuGet package.
```
dotnet add package ADX.Toolkit --version 1.0.0
```
## Control Command
```cs
var cluster = "<cluster>";
var database = "<database>";
var command = "<command>";
var appID = "<appID>";
var appSecret = "<appSecret>";
var appTenant = "<appTenant>";

var kustoHelper = new KustoHelper(4); // Four retries
IDataReader? dataReader = await kustoHelper.ExecuteCommandAsync(cluster, database, command, appID, appSecret, appTenant);
```

## Query
```cs
var cluster = "<cluster>";
var database = "<database>";
var query = "<query>";
var appID = "<appID>";
var appSecret = "<appSecret>";
var appTenant = "<appTenant>";

var kustoHelper = new KustoHelper(2); // Two retries (default)
IDataReader? dataReader = await kustoHelper.ExecuteQueryAsync(cluster, database, query, appID, appSecret, appTenant);
```
