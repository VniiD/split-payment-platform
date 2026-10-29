using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;

namespace SplitPayment.Domain.Tests;

public sealed class InvoiceApiTests : IClassFixture<InvoiceApiApplicationFactory>
{
    private readonly HttpClient _client;

    public InvoiceApiTests(InvoiceApiApplicationFactory factory)
    {
        _client = factory.CreateClient();
    }

    [Fact]
    public async Task InvoiceEndpoint_RejectsMissingApiKey()
    {
        using var response = await _client.PostAsJsonAsync("/api/v1/invoices", ValidInvoice);

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task InvoiceEndpoint_RequiresIdempotencyKey()
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "/api/v1/invoices")
        {
            Content = JsonContent.Create(ValidInvoice)
        };
        request.Headers.Add("X-API-Key", InvoiceApiApplicationFactory.TestApiKey);

        using var response = await _client.SendAsync(request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task InvoiceEndpoint_RejectsInvalidTaxDocumentsBeforePersistence()
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "/api/v1/invoices")
        {
            Content = JsonContent.Create(new
            {
                merchantTaxId = "12345678000199",
                customerTaxId = "98765432000155",
                grossAmount = 1000m,
                ibsRate = 17.7m,
                cbsRate = 8.8m,
                isSimplifiedProcedure = false
            })
        };
        request.Headers.Add("X-API-Key", InvoiceApiApplicationFactory.TestApiKey);
        request.Headers.Add("Idempotency-Key", "portfolio-test-0001");

        using var response = await _client.SendAsync(request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task OpenApiDocument_IsAvailable()
    {
        using var response = await _client.GetAsync("/openapi/v1.json");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var json = await response.Content.ReadAsStringAsync();
        Assert.Contains("/api/v1/invoices", json, StringComparison.Ordinal);
        Assert.Contains("/api/v2/demo/tax-calculations", json, StringComparison.Ordinal);
        Assert.Contains("X-API-Key", json, StringComparison.Ordinal);
        Assert.Contains("Idempotency-Key", json, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DemoTaxCalculation_ReturnsInformationalValuesAndDoesNotCollectTax()
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "/api/v2/demo/tax-calculations")
        {
            Content = JsonContent.Create(new
            {
                taxBase = 1000m,
                referenceDate = new DateOnly(2026, 10, 28)
            })
        };
        request.Headers.Add("X-API-Key", InvoiceApiApplicationFactory.TestApiKey);

        using var response = await _client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var result = await response.Content.ReadFromJsonAsync<TaxSimulationResponse>();
        Assert.NotNull(result);
        Assert.Equal("BR-RTC-2026-TEST-1", result.RuleSetId);
        Assert.Equal("InformationalTestOnly", result.Status);
        Assert.True(result.TaxBaseIsSynthetic);
        Assert.Equal(1m, result.IbsInformationalAmount);
        Assert.Equal(9m, result.CbsInformationalAmount);
        Assert.False(result.FiscalClassificationValidated);
        Assert.False(result.PaymentOrTaxCollectionExecuted);
    }

    [Fact]
    public async Task DemoTaxCalculation_RequiresApiKey()
    {
        using var response = await _client.PostAsJsonAsync(
            "/api/v2/demo/tax-calculations",
            new
            {
                taxBase = 1000m,
                referenceDate = new DateOnly(2026, 10, 28)
            });

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task DemoTaxCalculation_RejectsDateOutsideRuleSetPeriod()
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "/api/v2/demo/tax-calculations")
        {
            Content = JsonContent.Create(new
            {
                taxBase = 1000m,
                referenceDate = new DateOnly(2027, 1, 1)
            })
        };
        request.Headers.Add("X-API-Key", InvoiceApiApplicationFactory.TestApiKey);

        using var response = await _client.SendAsync(request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task DemoCheckout_RequiresApiKey()
    {
        using var response = await _client.PostAsJsonAsync(
            "/api/v2/demo/checkouts",
            new { productId = "demo-item-001", quantity = 1 });

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task DemoCheckout_StaysDisabledWithoutExplicitSandboxConfiguration()
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "/api/v2/demo/checkouts")
        {
            Content = JsonContent.Create(new { productId = "demo-item-001", quantity = 1 })
        };
        request.Headers.Add("X-API-Key", InvoiceApiApplicationFactory.TestApiKey);
        request.Headers.Add("Idempotency-Key", "demo-checkout-0001");

        using var response = await _client.SendAsync(request);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
    }

    [Fact]
    public async Task DemoProducts_ReturnFixedSyntheticCatalog()
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, "/api/v2/demo/products");
        request.Headers.Add("X-API-Key", InvoiceApiApplicationFactory.TestApiKey);

        using var response = await _client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var products = await response.Content.ReadAsStringAsync();
        Assert.Contains("demo-item-001", products, StringComparison.Ordinal);
        Assert.Contains("49.90", products, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DemoTestProfiles_AreHiddenOutsideLocalMockMode()
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, "/api/v2/demo/test-profiles");
        request.Headers.Add("X-API-Key", InvoiceApiApplicationFactory.TestApiKey);

        using var response = await _client.SendAsync(request);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task MockPayment_CannotBeUsedOutsideLocalMockMode()
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Post,
            $"/api/v2/demo/checkouts/{Guid.NewGuid():D}/mock-payment")
        {
            Content = JsonContent.Create(new { status = "approved" })
        };
        request.Headers.Add("X-API-Key", InvoiceApiApplicationFactory.TestApiKey);

        using var response = await _client.SendAsync(request);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    private static readonly object ValidInvoice = new
    {
        merchantTaxId = "11.222.333/0001-81",
        customerTaxId = "529.982.247-25",
        grossAmount = 1000m,
        ibsRate = 17.7m,
        cbsRate = 8.8m,
        isSimplifiedProcedure = false
    };

    private sealed record TaxSimulationResponse(
        string RuleSetId,
        string Status,
        bool TaxBaseIsSynthetic,
        decimal IbsInformationalAmount,
        decimal CbsInformationalAmount,
        bool FiscalClassificationValidated,
        bool PaymentOrTaxCollectionExecuted);
}

public sealed class InvoiceApiApplicationFactory : WebApplicationFactory<Program>
{
    public const string TestApiKey = "test-api-key-for-integration-tests-only-000000000000";
    private readonly string? _previousApiKey = Environment.GetEnvironmentVariable("Security__ApiKey");
    private readonly string? _previousDisableOutbox = Environment.GetEnvironmentVariable("Testing__DisableOutboxPublisher");
    private readonly string? _previousMongoConnection = Environment.GetEnvironmentVariable("Mongo__ConnectionString");
    private readonly string? _previousKafkaBootstrap = Environment.GetEnvironmentVariable("Kafka__BootstrapServers");

    public InvoiceApiApplicationFactory()
    {
        Environment.SetEnvironmentVariable("Security__ApiKey", TestApiKey);
        Environment.SetEnvironmentVariable("Testing__DisableOutboxPublisher", "true");
        Environment.SetEnvironmentVariable("Mongo__ConnectionString", "mongodb://localhost:27017");
        Environment.SetEnvironmentVariable("Kafka__BootstrapServers", "localhost:29092");
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.ConfigureAppConfiguration((_, configuration) =>
            configuration.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Security:ApiKey"] = TestApiKey,
                ["Testing:DisableOutboxPublisher"] = "true",
                ["Mongo:ConnectionString"] = "mongodb://localhost:27017",
                ["Kafka:BootstrapServers"] = "localhost:29092"
            }));
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Environment.SetEnvironmentVariable("Security__ApiKey", _previousApiKey);
            Environment.SetEnvironmentVariable("Testing__DisableOutboxPublisher", _previousDisableOutbox);
            Environment.SetEnvironmentVariable("Mongo__ConnectionString", _previousMongoConnection);
            Environment.SetEnvironmentVariable("Kafka__BootstrapServers", _previousKafkaBootstrap);
        }

        base.Dispose(disposing);
    }
}