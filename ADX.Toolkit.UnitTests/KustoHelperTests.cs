namespace ADX.Toolkit.UnitTests;

public class KustoHelperTests
{
    private readonly string cluster = "https://help.kusto.windows.net/";
    private readonly string database = "acme";
    private readonly string command = ".show tables";
    private readonly string query = "AcmeThings | take 10";
    private readonly string appID = Guid.NewGuid().ToString();
    private readonly string appSecret = Guid.NewGuid().ToString();
    private readonly string appTenant = Guid.NewGuid().ToString();

    [Fact]
    public void KustoHelper_RetriesSetToZero_ThrowsArgumentOutOfRangeException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            var kH = new KustoHelper(0);
        });
    }

    [Fact]
    public void KustoHelper_RetriesSetToLessThanZero_ThrowsArgumentOutOfRangeException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            var kH = new KustoHelper(-2);
        });
    }

    [Fact]
    public void KustoHelper_RetriesSetToGreaterThanMaxRetries_ThrowsArgumentOutOfRangeException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            var kH = new KustoHelper(KustoHelper.MaxRetries + 3);
        });
    }

    [Fact]
    public void KustoHelper_ExecuteCommand_ClusterIsNull_ArgumentNullExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteCommandAsync(null, database, command, appID, appSecret, appTenant);

        Assert.ThrowsAsync<ArgumentNullException>(action);
    }

    [Fact]
    public void KustoHelper_ExecuteCommand_DatabaseIsNull_ArgumentNullExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteCommandAsync(cluster, null, command, appID, appSecret, appTenant);

        Assert.ThrowsAsync<ArgumentNullException>(action);
    }

    [Fact]
    public void KustoHelper_ExecuteCommand_CommandIsNull_ArgumentNullExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteCommandAsync(cluster, database, null, appID, appSecret, appTenant);

        Assert.ThrowsAsync<ArgumentNullException>(action);
    }

    [Fact]
    public void KustoHelper_ExecuteCommand_AppIdIsNull_ArgumentNullExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteCommandAsync(cluster, database, command, null, appSecret, appTenant);

        Assert.ThrowsAsync<ArgumentNullException>(action);
    }

    [Fact]
    public void KustoHelper_ExecuteCommand_AppSecretIsNull_ArgumentNullExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteCommandAsync(cluster, database, command, appID, null, appTenant);

        Assert.ThrowsAsync<ArgumentNullException>(action);
    }

    [Fact]
    public void KustoHelper_ExecuteCommand_AppTenantIsNull_ArgumentNullExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteCommandAsync(cluster, database, command, appID, appSecret, null);

        Assert.ThrowsAsync<ArgumentNullException>(action);
    }

    [Fact]
    public void KustoHelper_ExecuteCommand_ClusterIsEmpty_ArgumentExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteCommandAsync(string.Empty, database, command, appID, appSecret, appTenant);

        Assert.ThrowsAsync<ArgumentException>(action);
    }

    [Fact]
    public void KustoHelper_ExecuteCommand_DatabaseIsEmpty_ArgumentExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteCommandAsync(cluster, string.Empty, command, appID, appSecret, appTenant);

        Assert.ThrowsAsync<ArgumentException>(action);
    }

    [Fact]
    public void KustoHelper_ExecuteCommand_CommandIsEmpty_ArgumentExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteCommandAsync(cluster, database, string.Empty, appID, appSecret, appTenant);

        Assert.ThrowsAsync<ArgumentException>(action);
    }

    [Fact]
    public void KustoHelper_ExecuteCommand_AppIdIsEmpty_ArgumentExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteCommandAsync(cluster, database, command, string.Empty, appSecret, appTenant);

        Assert.ThrowsAsync<ArgumentException>(action);
    }

    [Fact]
    public void KustoHelper_ExecuteCommand_AppSecretIsEmpty_ArgumentExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteCommandAsync(cluster, database, command, appID, string.Empty, appTenant);

        Assert.ThrowsAsync<ArgumentException>(action);
    }

    [Fact]
    public void KustoHelper_ExecuteCommand_AppTenantIsEmpty_ArgumentExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteCommandAsync(cluster, database, command, appID, appSecret, string.Empty);

        Assert.ThrowsAsync<ArgumentException>(action);
    }

    [Fact]
    public void KustoHelper_ExecuteQuery_ClusterIsNull_ArgumentNullExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteQueryAsync(null, database, query, appID, appSecret, appTenant);

        Assert.ThrowsAsync<ArgumentNullException>(action);
    }

    [Fact]
    public void KustoHelper_ExecuteQuery_DatabaseIsNull_ArgumentNullExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteQueryAsync(cluster, null, query, appID, appSecret, appTenant);

        Assert.ThrowsAsync<ArgumentNullException>(action);
    }

    [Fact]
    public void KustoHelper_ExecuteQuery_QueryIsNull_ArgumentNullExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteQueryAsync(cluster, database, null, appID, appSecret, appTenant);

        Assert.ThrowsAsync<ArgumentNullException>(action);
    }

    [Fact]
    public void KustoHelper_ExecuteQuery_AppIdIsNull_ArgumentNullExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteQueryAsync(cluster, database, query, null, appSecret, appTenant);

        Assert.ThrowsAsync<ArgumentNullException>(action);
    }

    [Fact]
    public void KustoHelper_ExecuteQuery_AppSecretIsNull_ArgumentNullExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteQueryAsync(cluster, database, query, appID, null, appTenant);

        Assert.ThrowsAsync<ArgumentNullException>(action);
    }

    [Fact]
    public void KustoHelper_ExecuteQuery_AppTenantIsNull_ArgumentNullExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteQueryAsync(cluster, database, query, appID, appSecret, null);

        Assert.ThrowsAsync<ArgumentNullException>(action);
    }

    [Fact]
    public void KustoHelper_ExecuteQuery_ClusterIsEmpty_ArgumentExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteQueryAsync(string.Empty, database, query, appID, appSecret, appTenant);

        Assert.ThrowsAsync<ArgumentException>(action);
    }

    [Fact]
    public void KustoHelper_ExecuteQuery_DatabaseIsEmpty_ArgumentExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteQueryAsync(cluster, string.Empty, query, appID, appSecret, appTenant);

        Assert.ThrowsAsync<ArgumentException>(action);
    }

    [Fact]
    public void KustoHelper_ExecuteQuery_QueryIsEmpty_ArgumentExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteQueryAsync(cluster, database, string.Empty, appID, appSecret, appTenant);

        Assert.ThrowsAsync<ArgumentException>(action);
    }

    [Fact]
    public void KustoHelper_ExecuteQuery_AppIdIsEmpty_ArgumentExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteQueryAsync(cluster, database, query, string.Empty, appSecret, appTenant);

        Assert.ThrowsAsync<ArgumentException>(action);
    }

    [Fact]
    public void KustoHelper_ExecuteQuery_AppSecretIsEmpty_ArgumentExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteQueryAsync(cluster, database, query, appID, string.Empty, appTenant);

        Assert.ThrowsAsync<ArgumentException>(action);
    }

    [Fact]
    public void KustoHelper_ExecuteQuery_AppTenantIsEmpty_ArgumentExceptionThrown()
    {
        var kustoHelper = new KustoHelper();

        var action = async () => await kustoHelper.ExecuteQueryAsync(cluster, database, query, appID, appSecret, string.Empty);

        Assert.ThrowsAsync<ArgumentException>(action);
    }
}
