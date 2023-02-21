# ADX.Toolkit
Kusto client wrapper for executing control commands and queries with retries using exponential backoff.

# Usage
Install NuGet package.
```
dotnet add package ADX.Toolkit --version 1.2.0
```
## Control Command
```cs
var cluster = "<cluster>";
var database = "<database>";
var command = "<command>";
var appID = "<appID>";
var appSecret = "<appSecret>";
var appTenant = "<appTenant>";

var kustoHelper = new KustoHelper();
kustoHelper.BaseWaitTime = 4; // Base wait time for retries changed to 4 sec.
IDataReader? dataReader = await kustoHelper.ExecuteCommandAsync(cluster, database, command, appID, appSecret, appTenant, 3); // 3 retries
```

## Query
```cs
var cluster = "<cluster>";
var database = "<database>";
var query = "<query>";
var appID = "<appID>";
var appSecret = "<appSecret>";
var appTenant = "<appTenant>";

var kustoHelper = new KustoHelper();
IDataReader? dataReader = await kustoHelper.ExecuteQueryAsync(cluster, database, query, appID, appSecret, appTenant);
```
