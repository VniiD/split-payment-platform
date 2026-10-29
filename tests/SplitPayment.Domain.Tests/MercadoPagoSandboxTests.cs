using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;
using SplitPayment.InvoiceApi.Payments;

namespace SplitPayment.Domain.Tests;

public sealed class MercadoPagoSandboxTests
{
    [Fact]
    public async Task CreatePreference_UsesTestTokenAndReturnsOnlySandboxUrl()
    {
        string? capturedToken = null;
        string? capturedBody = null;
        var handler = new StubHttpMessageHandler(async (request, cancellationToken) =>
        {
            capturedToken = request.Headers.Authorization?.Parameter;
            capturedBody = await request.Content!.ReadAsStringAsync(cancellationToken);
            return new HttpResponseMessage(HttpStatusCode.Created)
            {
                Content = new StringContent(
                    """{"id":"preference-123","sandbox_init_point":"https://sandbox.mercadopago.com/checkout/v1/redirect?pref_id=preference-123","init_point":"https://www.mercadopago.com/checkout/v1/redirect?pref_id=preference-123"}""",
                    Encoding.UTF8,
                    "application/json")
            };
        });
        using var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri("https://api.mercadopago.com/")
        };
        var client = CreateClient(httpClient);
        var checkout = new DemoCheckoutDocument
        {
            CheckoutId = Guid.NewGuid().ToString("D"),
            ProductId = "demo-item-001",
            ProductName = "Produto demonstrativo",
            UnitPrice = 49.90m,
            Quantity = 2,
            TotalAmount = 99.80m
        };

        var preference = await client.CreatePreferenceAsync(checkout, CancellationToken.None);

        Assert.Equal("preference-123", preference.Id);
        Assert.StartsWith("https://sandbox.mercadopago.com/", preference.SandboxInitPoint, StringComparison.Ordinal);
        Assert.Equal("TEST-sandbox-token", capturedToken);
        Assert.NotNull(capturedBody);
        using var body = JsonDocument.Parse(capturedBody);
        var item = body.RootElement.GetProperty("items")[0];
        Assert.Equal(2, item.GetProperty("quantity").GetInt32());
        Assert.Equal(49.90m, item.GetProperty("unit_price").GetDecimal());
        Assert.Equal("BRL", item.GetProperty("currency_id").GetString());
        var checkoutId = checkout.CheckoutId;
        var backUrls = body.RootElement.GetProperty("back_urls");
        Assert.Equal($"http://localhost:5081/checkout/result/{checkoutId}?result=success",
            backUrls.GetProperty("success").GetString());
        Assert.Equal("approved", body.RootElement.GetProperty("auto_return").GetString());
    }

    [Fact]
    public async Task CreatePreference_RejectsProductionCheckoutUrl()
    {
        var handler = new StubHttpMessageHandler((_, _) =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(
                    """{"id":"preference-123","sandbox_init_point":"https://attacker.example/checkout","init_point":"https://www.mercadopago.com/checkout"}""",
                    Encoding.UTF8,
                    "application/json")
            }));
        using var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri("https://api.mercadopago.com/")
        };

        await Assert.ThrowsAsync<InvalidDataException>(
            () => CreateClient(httpClient).CreatePreferenceAsync(
                new DemoCheckoutDocument { CheckoutId = Guid.NewGuid().ToString("D") },
                CancellationToken.None));
    }

    [Fact]
    public async Task GetPayment_RequiresMatchingSandboxPaymentAndReturnsProviderStatus()
    {
        var handler = new StubHttpMessageHandler((request, _) =>
        {
            Assert.Equal(HttpMethod.Get, request.Method);
            Assert.Equal("/v1/payments/12345", request.RequestUri?.AbsolutePath);
            Assert.Equal("TEST-sandbox-token", request.Headers.Authorization?.Parameter);
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(
                    """{"id":12345,"external_reference":"checkout-id","status":"approved","transaction_amount":49.90,"currency_id":"BRL","live_mode":false}""",
                    Encoding.UTF8,
                    "application/json")
            });
        });
        using var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri("https://api.mercadopago.com/")
        };

        var result = await CreateClient(httpClient).GetPaymentAsync("12345", CancellationToken.None);

        Assert.Equal("12345", result.Id);
        Assert.Equal("checkout-id", result.ExternalReference);
        Assert.Equal("approved", result.Status);
        Assert.Equal(49.90m, result.TransactionAmount);
    }

    [Fact]
    public void WebhookValidator_AcceptsFreshSignatureAndRejectsTamperingOrReplay()
    {
        const string secret = "local-webhook-test-secret";
        const string requestId = "request-123";
        const string dataId = "987654321";
        var now = DateTimeOffset.UtcNow;
        var timestamp = now.ToUnixTimeMilliseconds().ToString(System.Globalization.CultureInfo.InvariantCulture);
        var manifest = $"id:{dataId};request-id:{requestId};ts:{timestamp};";
        var hash = Convert.ToHexString(HMACSHA256.HashData(
            Encoding.UTF8.GetBytes(secret),
            Encoding.UTF8.GetBytes(manifest))).ToLowerInvariant();
        var signature = $"ts={timestamp},v1={hash}";

        Assert.True(MercadoPagoWebhookSignatureValidator.IsValid(
            signature, requestId, dataId, secret, now));
        Assert.False(MercadoPagoWebhookSignatureValidator.IsValid(
            signature, "different-request", dataId, secret, now));
        Assert.False(MercadoPagoWebhookSignatureValidator.IsValid(
            signature, requestId, dataId, secret, now.AddMinutes(6)));
        Assert.False(MercadoPagoWebhookSignatureValidator.IsValid(
            "ts=bad,v1=00", requestId, dataId, secret, now));
    }

    [Fact]
    public void SandboxOptions_RequireTestTokenAndHttpsWebhook()
    {
        Assert.True(new MercadoPagoSandboxOptions
        {
            Mode = "Sandbox",
            AccessToken = "TEST-local-token",
            WebhookSecret = "secret",
            NotificationUrl = "https://example.test/webhooks/mercadopago/payments",
            StorefrontBaseUrl = "https://example.test"
        }.IsEnabled);

        Assert.False(new MercadoPagoSandboxOptions
        {
            Mode = "Sandbox",
            AccessToken = "APP-live-token",
            WebhookSecret = "secret",
            NotificationUrl = "https://example.test/webhooks/mercadopago/payments",
            StorefrontBaseUrl = "https://example.test"
        }.IsEnabled);
        Assert.False(new MercadoPagoSandboxOptions
        {
            Mode = "Sandbox",
            AccessToken = "TEST-local-token",
            WebhookSecret = "secret",
            NotificationUrl = "http://localhost:5080/webhooks/mercadopago/payments",
            StorefrontBaseUrl = "https://example.test"
        }.IsEnabled);
        Assert.False(new MercadoPagoSandboxOptions
        {
            Mode = "Sandbox",
            AccessToken = "TEST-local-token",
            WebhookSecret = "secret",
            NotificationUrl = "https://example.test/other-path",
            StorefrontBaseUrl = "https://example.test"
        }.IsEnabled);
        Assert.False(new MercadoPagoSandboxOptions
        {
            Mode = "Sandbox",
            AccessToken = "TEST-local-token",
            WebhookSecret = "secret",
            NotificationUrl = "https://example.test/webhooks/mercadopago/payments",
            StorefrontBaseUrl = "http://checkout-attacker.example"
        }.IsEnabled);
    }

    private static MercadoPagoSandboxClient CreateClient(HttpClient httpClient) =>
        new(httpClient, Options.Create(new MercadoPagoSandboxOptions
        {
            Mode = "Sandbox",
            AccessToken = "TEST-sandbox-token",
            WebhookSecret = "local-webhook-test-secret",
            NotificationUrl = "https://example.test/webhooks/mercadopago/payments",
            StorefrontBaseUrl = "http://localhost:5081"
        }));

    private sealed class StubHttpMessageHandler(
        Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> send) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken) => send(request, cancellationToken);
    }
}
