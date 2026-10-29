using System.Net;
using System.Net.Http.Json;

namespace SplitPayment.Web.Services;

public sealed class DemoCheckoutApi(HttpClient httpClient)
{
    public async Task<IReadOnlyList<DemoProduct>> GetProductsAsync(CancellationToken cancellationToken)
    {
        using var response = await httpClient.GetAsync("api/v2/demo/products", cancellationToken);
        await EnsureSuccessAsync(response, cancellationToken);
        return await response.Content.ReadFromJsonAsync<List<DemoProduct>>(cancellationToken)
            ?? throw new InvalidDataException("A API não retornou o catálogo de demonstração.");
    }

    public async Task<IReadOnlyList<DemoTestProfile>> GetTestProfilesAsync(CancellationToken cancellationToken)
    {
        using var response = await httpClient.GetAsync("api/v2/demo/test-profiles", cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return [];
        }

        await EnsureSuccessAsync(response, cancellationToken);
        return await response.Content.ReadFromJsonAsync<List<DemoTestProfile>>(cancellationToken)
            ?? throw new InvalidDataException("A API não retornou os perfis de teste.");
    }

    public async Task<DemoCheckout> CreateCheckoutAsync(
        string productId,
        int quantity,
        string? testProfileId,
        string idempotencyKey,
        CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "api/v2/demo/checkouts")
        {
            Content = JsonContent.Create(new CreateDemoCheckoutRequest(productId, quantity, testProfileId))
        };
        request.Headers.Add("Idempotency-Key", idempotencyKey);
        using var response = await httpClient.SendAsync(request, cancellationToken);
        await EnsureSuccessAsync(response, cancellationToken);
        var checkout = await response.Content.ReadFromJsonAsync<DemoCheckout>(cancellationToken)
            ?? throw new InvalidDataException("A API não retornou os dados do checkout.");
        var validCheckoutUrl = checkout.IsMockPayment
            ? checkout.MockCheckoutUrl == $"/checkout/mock/{checkout.CheckoutId}"
            : IsSandboxCheckoutUrl(checkout.SandboxCheckoutUrl);
        if (!validCheckoutUrl)
        {
            throw new InvalidDataException("A API não retornou uma URL de checkout válida para o modo informado.");
        }

        return checkout;
    }

    public async Task<DemoCheckoutStatus> GetCheckoutStatusAsync(
        Guid checkoutId,
        CancellationToken cancellationToken)
    {
        using var response = await httpClient.GetAsync(
            $"api/v2/demo/checkouts/{checkoutId:D}",
            cancellationToken);
        await EnsureSuccessAsync(response, cancellationToken);
        return await response.Content.ReadFromJsonAsync<DemoCheckoutStatus>(cancellationToken)
            ?? throw new InvalidDataException("A API não retornou o status do checkout.");
    }

    public async Task CompleteMockPaymentAsync(
        Guid checkoutId,
        string status,
        CancellationToken cancellationToken)
    {
        using var response = await httpClient.PostAsJsonAsync(
            $"api/v2/demo/checkouts/{checkoutId:D}/mock-payment",
            new CompleteMockPaymentRequest(status),
            cancellationToken);
        await EnsureSuccessAsync(response, cancellationToken);
    }

    private static async Task EnsureSuccessAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.IsSuccessStatusCode)
        {
            return;
        }

        var statusCode = response.StatusCode;
        var detail = statusCode switch
        {
            HttpStatusCode.ServiceUnavailable => "Checkout sandbox ainda não foi configurado.",
            HttpStatusCode.NotFound => "O checkout não foi encontrado.",
            HttpStatusCode.Conflict => "O checkout está sendo processado; tente consultar novamente.",
            HttpStatusCode.TooManyRequests => "Muitas tentativas em sequência. Aguarde um momento.",
            _ => "Não foi possível concluir a solicitação agora."
        };
        throw new DemoCheckoutApiException(statusCode, detail);
    }

    private static bool IsSandboxCheckoutUrl(string? value) =>
        Uri.TryCreate(value, UriKind.Absolute, out var uri) &&
        uri.Scheme == Uri.UriSchemeHttps &&
        (uri.Host.Equals("sandbox.mercadopago.com", StringComparison.OrdinalIgnoreCase) ||
         uri.Host.Equals("sandbox.mercadopago.com.br", StringComparison.OrdinalIgnoreCase));
}

public sealed record DemoProduct(string Id, string Name, string Description, decimal UnitPrice);

public sealed record DemoTestProfile(string Id, string DisplayName, string Email, bool IsSynthetic);

public sealed record CreateDemoCheckoutRequest(string ProductId, int Quantity, string? TestProfileId);

public sealed record DemoCheckout(
    string CheckoutId,
    string ProductId,
    string ProductName,
    decimal UnitPrice,
    int Quantity,
    decimal TotalAmount,
    string Status,
    string? ProviderPreferenceId,
    string? SandboxCheckoutUrl,
    bool IsSandbox,
    string? MockCheckoutUrl,
    string? TestProfileName,
    bool IsMockPayment);

public sealed record DemoCheckoutStatus(
    string CheckoutId,
    string ProductId,
    string ProductName,
    decimal UnitPrice,
    int Quantity,
    decimal TotalAmount,
    string Status,
    string? ProviderPaymentId,
    DateTime CreatedAt,
    DateTime UpdatedAt,
    bool IsSandbox,
    bool IsMockPayment,
    string? TestProfileName);

public sealed record CompleteMockPaymentRequest(string Status);

public sealed class DemoCheckoutApiException(HttpStatusCode statusCode, string message)
    : Exception(message)
{
    public HttpStatusCode StatusCode { get; } = statusCode;
}
