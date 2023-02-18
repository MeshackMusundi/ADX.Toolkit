using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Data;
using Polly;
using System.Data;
using CommunityToolkit.Diagnostics;

namespace ADX.Toolkit;

public class KustoHelper
{
    private readonly int _retries;
    
    public static int MaxRetries { get; } = 5;
    public static int MinRetries { get; } = 0;

    private int _baseWaitTime = 2;
    /// <summary>
    /// The base wait time for retries in seconds.
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

    /// <param name="retries">Number of times a command/query execution should be retried.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if retries value is less than zero
    /// or greater than 5.</exception>
    public KustoHelper(int retries = 2)
    {
        Guard.IsGreaterThanOrEqualTo(retries, MinRetries);
        Guard.IsLessThanOrEqualTo(retries, MaxRetries);

        _retries = retries;
    }

    private void ValidateParameters(string cluster, string database, string appID, string appKey, string appTenant)
    {
        Guard.IsNotNullOrEmpty(cluster);
        Guard.IsNotNullOrEmpty(database);
        Guard.IsNotNullOrEmpty(appID);
        Guard.IsNotNullOrEmpty(appKey);
        Guard.IsNotNullOrEmpty(appTenant);
    }

    /// <summary>
    /// Executes a Kusto control command. Retries are executed with exponential backoff
    /// with a base wait time of 2 sec.
    /// </summary>
    /// <param name="cluster">The Kusto cluster URL.</param>
    /// <param name="database">The database name.</param>
    /// <param name="command">The control command statement to execute.</param>
    /// <param name="appID">The application (client) ID.</param>
    /// <param name="appKey">The application secret.</param>
    /// <param name="appTenant">The application tenant.</param>
    public Task<IDataReader?> ExecuteCommandAsync(string cluster, string database, string command, string appID, string appKey, string appTenant,
        CancellationToken cancellationToken = default)
    {
        ValidateParameters(cluster, database, appID, appKey, appTenant);

        Guard.IsNotNullOrEmpty(command);

        var connectionStringBuilder = new KustoConnectionStringBuilder(cluster, database)
                    .WithAadApplicationKeyAuthentication(appID, appKey, appTenant);

        using var commandProvider = KustoClientFactory.CreateCslAdminProvider(connectionStringBuilder);
        var requestProperties = new ClientRequestProperties() { ClientRequestId = Guid.NewGuid().ToString() };

        var retryPolicy = Policy.Handle<Exception>()
            .WaitAndRetryAsync(_retries, retryAttempt => TimeSpan.FromSeconds(Math.Pow(BaseWaitTime, retryAttempt)));

        var dataReader = retryPolicy.ExecuteAsync((ct)
            => commandProvider.ExecuteControlCommandAsync(database, command, requestProperties), cancellationToken);

        return dataReader;
    }

    /// <summary>
    /// Executes a Kusto query. Retries are executed with exponential backoff
    /// with a base wait time of 2 sec.
    /// </summary>
    /// <param name="cluster">The Kusto cluster URL.</param>
    /// <param name="database">The database name.</param>
    /// <param name="query">The Kusto query statement to execute.</param>
    /// <param name="appID">The application (client) ID.</param>
    /// <param name="appKey">The application secret.</param>
    /// <param name="appTenant">The application tenant.</param>
    public Task<IDataReader?> ExecuteQueryAsync(string cluster, string database, string query, string appID, string appKey, string appTenant,
        CancellationToken cancellationToken = default)
    {
        ValidateParameters(cluster, database, appID, appKey, appTenant);

        Guard.IsNotNullOrEmpty(query);

        var connectionStringBuilder = new KustoConnectionStringBuilder(cluster, database)
                    .WithAadApplicationKeyAuthentication(appID, appKey, appTenant);

        using var queryProvider = KustoClientFactory.CreateCslQueryProvider(connectionStringBuilder);
        var clientRequestProperties = new ClientRequestProperties() { ClientRequestId = Guid.NewGuid().ToString() };

        var retryPolicy = Policy.Handle<Exception>()
            .WaitAndRetryAsync(_retries, retryAttempt => TimeSpan.FromSeconds(Math.Pow(BaseWaitTime, retryAttempt)));

        var dataReader = retryPolicy.ExecuteAsync((ct)
            => queryProvider.ExecuteQueryAsync(database, query, clientRequestProperties), cancellationToken);

        return dataReader;
    }
}
