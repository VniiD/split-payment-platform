using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Options;

namespace SplitPayment.InvoiceApi.Payments;

public sealed class MercadoPagoSandboxClient
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly HttpClient _httpClient;
    private readonly MercadoPagoSandboxOptions _options;

    public MercadoPagoSandboxClient(HttpClient httpClient, IOptions<MercadoPagoSandboxOptions> options)
    {
        _httpClient = httpClient;
        _options = options.Value;
    }

    public bool IsEnabled => _options.IsEnabled;

    public async Task<CheckoutPreference> CreatePreferenceAsync(
        DemoCheckoutDocument checkout,
        CancellationToken cancellationToken)
    {
        EnsureEnabled();

        using var request = new HttpRequestMessage(HttpMethod.Post, "checkout/preferences");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _options.AccessToken);
        request.Content = JsonContent.Create(new
        {
            items = new[]
            {
                new
                {
                    id = checkout.ProductId,
                    title = checkout.ProductName,
                    description = "Compra de teste em ambiente sandbox.",
                    quantity = checkout.Quantity,
                    currency_id = "BRL",
                    unit_price = checkout.UnitPrice
                }
            },
            external_reference = checkout.CheckoutId,
            notification_url = _options.NotificationUrl,
            back_urls = new
            {
                success = GetReturnUrl(checkout.CheckoutId, "success"),
                pending = GetReturnUrl(checkout.CheckoutId, "pending"),
                failure = GetReturnUrl(checkout.CheckoutId, "failure")
            },
            auto_return = "approved",
            metadata = new { checkout_id = checkout.CheckoutId },
            payment_methods = new
            {
                excluded_payment_types = new[]
                {
                    new { id = "ticket" },
                    new { id = "atm" }
                }
            }
        }, options: JsonOptions);

        using var response = await _httpClient.SendAsync(request, cancellationToken);
        if (!response.IsSuccessStatusCode)
        {
            throw new MercadoPagoProviderException(response.StatusCode);
        }

        PreferenceResponse? preference;
        try
        {
            preference = await response.Content.ReadFromJsonAsync<PreferenceResponse>(JsonOptions, cancellationToken);
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException("Mercado Pago returned an unreadable preference response.", exception);
        }
        if (preference is null ||
            string.IsNullOrWhiteSpace(preference.Id) ||
            !IsMercadoPagoSandboxUrl(preference.SandboxInitPoint))
        {
            throw new InvalidDataException("Mercado Pago did not return a valid sandbox checkout URL.");
        }

        return new CheckoutPreference(preference.Id, preference.SandboxInitPoint);
    }

    public async Task<ProviderPayment> GetPaymentAsync(string paymentId, CancellationToken cancellationToken)
    {
        EnsureEnabled();
        if (paymentId.Length is < 1 or > 32 || !paymentId.All(char.IsAsciiDigit))
        {
            throw new ArgumentException("Payment ID must be numeric.", nameof(paymentId));
        }

        using var request = new HttpRequestMessage(HttpMethod.Get, $"v1/payments/{paymentId}");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _options.AccessToken);
        using var response = await _httpClient.SendAsync(request, cancellationToken);
        if (!response.IsSuccessStatusCode)
        {
            throw new MercadoPagoProviderException(response.StatusCode);
        }

        PaymentResponse? payment;
        try
        {
            payment = await response.Content.ReadFromJsonAsync<PaymentResponse>(JsonOptions, cancellationToken);
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException("Mercado Pago returned an unreadable payment response.", exception);
        }
        if (payment is null ||
            string.IsNullOrWhiteSpace(payment.ExternalReference) ||
            string.IsNullOrWhiteSpace(payment.Status) ||
            payment.LiveMode is not false ||
            !string.Equals(payment.CurrencyId, "BRL", StringComparison.Ordinal))
        {
            throw new InvalidDataException("Mercado Pago returned an incomplete payment record.");
        }

        var id = payment.Id.ValueKind switch
        {
            JsonValueKind.Number => payment.Id.GetRawText(),
            JsonValueKind.String => payment.Id.GetString() ?? string.Empty,
            _ => string.Empty
        };
        if (id != paymentId)
        {
            throw new InvalidDataException("Mercado Pago returned a payment ID that does not match the request.");
        }

        return new ProviderPayment(id, payment.ExternalReference, payment.Status, payment.TransactionAmount);
    }

    private void EnsureEnabled()
    {
        if (!IsEnabled)
        {
            throw new InvalidOperationException(
                "Mercado Pago sandbox is disabled or incompletely configured. Configure test credentials and an HTTPS notification URL.");
        }
    }

    private string GetReturnUrl(string checkoutId, string result) =>
        $"{_options.StorefrontBaseUrl.TrimEnd('/')}/checkout/result/{checkoutId}?result={result}";

    private static bool IsMercadoPagoSandboxUrl(string value)
    {
        if (!Uri.TryCreate(value, UriKind.Absolute, out var uri) || uri.Scheme != Uri.UriSchemeHttps)
        {
            return false;
        }

        var host = uri.Host;
        return host.Equals("sandbox.mercadopago.com", StringComparison.OrdinalIgnoreCase) ||
               host.Equals("sandbox.mercadopago.com.br", StringComparison.OrdinalIgnoreCase);
    }

    private sealed record PreferenceResponse(
        string Id,
        [property: JsonPropertyName("sandbox_init_point")] string SandboxInitPoint);

    private sealed record PaymentResponse(
        JsonElement Id,
        [property: JsonPropertyName("external_reference")] string ExternalReference,
        string Status,
        [property: JsonPropertyName("transaction_amount")] decimal TransactionAmount,
        [property: JsonPropertyName("currency_id")] string CurrencyId,
        [property: JsonPropertyName("live_mode")] bool? LiveMode);
}

public sealed record CheckoutPreference(string Id, string SandboxInitPoint);

public sealed record ProviderPayment(string Id, string ExternalReference, string Status, decimal TransactionAmount);

public sealed class MercadoPagoProviderException(HttpStatusCode statusCode)
    : Exception($"Mercado Pago sandbox returned HTTP {(int)statusCode}.")
{
    public HttpStatusCode StatusCode { get; } = statusCode;
}
