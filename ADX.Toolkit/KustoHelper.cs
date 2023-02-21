using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Data;
using Polly;
using System.Data;
using CommunityToolkit.Diagnostics;

namespace ADX.Toolkit;

public class KustoHelper
{    
    public static int MaxRetries { get; } = 10;
    public static int MinRetries { get; } = 1;

    private int _baseWaitTime = 2;
    /// <summary>
    /// The base wait time for retries, in seconds.
    /// </summary>
    public int BaseWaitTime
    {
        get => _baseWaitTime;
        set
        { 
            if (value < 0) throw new ArgumentOutOfRangeException(nameof(BaseWaitTime));
            _baseWaitTime = value; 
        }
    }

    private void ValidateParameters(string cluster, string database, string appID, string appKey, string appTenant, int retries)
    {
        Guard.IsNotNullOrEmpty(cluster);
        Guard.IsNotNullOrEmpty(database);
        Guard.IsNotNullOrEmpty(appID);
        Guard.IsNotNullOrEmpty(appKey);
        Guard.IsNotNullOrEmpty(appTenant);
        Guard.IsInRange(retries, MinRetries, MaxRetries);
    }

    /// <summary>
    /// Executes a Kusto control command. Retries are executed with exponential backoff
    /// with a base wait time, in seconds, of <see cref="BaseWaitTime"/>.
    /// </summary>
    /// <param name="cluster">The Kusto cluster URL.</param>
    /// <param name="database">The database name.</param>
    /// <param name="command">The control command statement to execute.</param>
    /// <param name="appID">The application (client) ID.</param>
    /// <param name="appKey">The application secret.</param>
    /// <param name="appTenant">The application tenant.</param>
    /// <param name="retries">The number of retry executions.</param>
    public async Task<IDataReader?> ExecuteCommandAsync(string cluster, string database, string command, string appID, string appKey, string appTenant,
        int retries = 2, CancellationToken cancellationToken = default)
    {
        ValidateParameters(cluster, database, appID, appKey, appTenant, retries);
        Guard.IsNotNullOrEmpty(command);

        var connectionStringBuilder = new KustoConnectionStringBuilder(cluster, database)
                    .WithAadApplicationKeyAuthentication(appID, appKey, appTenant);

        using var commandProvider = KustoClientFactory.CreateCslAdminProvider(connectionStringBuilder);
        var requestProperties = new ClientRequestProperties() { ClientRequestId = Guid.NewGuid().ToString() };

        var retryPolicy = Policy.Handle<Exception>()
            .WaitAndRetryAsync(retries, retryAttempt => TimeSpan.FromSeconds(Math.Pow(BaseWaitTime, retryAttempt)));

        var dataReader = await retryPolicy.ExecuteAsync(async (ct)
            => await commandProvider.ExecuteControlCommandAsync(database, command, requestProperties), cancellationToken);

        return dataReader;
    }

    /// <summary>
    /// Executes a Kusto query. Retries are executed with exponential backoff
    /// with a base wait time, in seconds, of <see cref="BaseWaitTime"/>.
    /// </summary>
    /// <param name="cluster">The Kusto cluster URL.</param>
    /// <param name="database">The database name.</param>
    /// <param name="query">The Kusto query statement to execute.</param>
    /// <param name="appID">The application (client) ID.</param>
    /// <param name="appKey">The application secret.</param>
    /// <param name="appTenant">The application tenant.</param>
    /// <param name="retries">The number of retry executions.</param>
    public async Task<IDataReader?> ExecuteQueryAsync(string cluster, string database, string query, string appID, string appKey, string appTenant,
        int retries = 2, CancellationToken cancellationToken = default)
    {
        ValidateParameters(cluster, database, appID, appKey, appTenant, retries);
        Guard.IsNotNullOrEmpty(query);

        var connectionStringBuilder = new KustoConnectionStringBuilder(cluster, database)
                    .WithAadApplicationKeyAuthentication(appID, appKey, appTenant);

        using var queryProvider = KustoClientFactory.CreateCslQueryProvider(connectionStringBuilder);
        var clientRequestProperties = new ClientRequestProperties() { ClientRequestId = Guid.NewGuid().ToString() };

        var retryPolicy = Policy.Handle<Exception>()
            .WaitAndRetryAsync(retries, retryAttempt => TimeSpan.FromSeconds(Math.Pow(BaseWaitTime, retryAttempt)));

        var dataReader = await retryPolicy.ExecuteAsync(async (ct)
            => await queryProvider.ExecuteQueryAsync(database, query, clientRequestProperties), cancellationToken);

        return dataReader;
    }
}
