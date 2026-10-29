namespace SplitPayment.InvoiceApi.Payments;

public sealed class MercadoPagoSandboxOptions
{
    public string Mode { get; set; } = "Disabled";
    public string AccessToken { get; set; } = string.Empty;
    public string WebhookSecret { get; set; } = string.Empty;
    public string NotificationUrl { get; set; } = string.Empty;
    public string StorefrontBaseUrl { get; set; } = string.Empty;

    public bool IsEnabled =>
        string.Equals(Mode, "Sandbox", StringComparison.Ordinal) &&
        AccessToken.StartsWith("TEST-", StringComparison.Ordinal) &&
        !string.IsNullOrWhiteSpace(WebhookSecret) &&
        Uri.TryCreate(NotificationUrl, UriKind.Absolute, out var notificationUri) &&
        notificationUri.Scheme == Uri.UriSchemeHttps &&
        notificationUri.AbsolutePath.EndsWith("/webhooks/mercadopago/payments", StringComparison.Ordinal) &&
        string.IsNullOrEmpty(notificationUri.UserInfo) &&
        string.IsNullOrEmpty(notificationUri.Fragment) &&
        Uri.TryCreate(StorefrontBaseUrl, UriKind.Absolute, out var storefrontUri) &&
        (storefrontUri.Scheme == Uri.UriSchemeHttps ||
         storefrontUri.IsLoopback && storefrontUri.Scheme == Uri.UriSchemeHttp) &&
        string.IsNullOrEmpty(storefrontUri.UserInfo) &&
        string.IsNullOrEmpty(storefrontUri.Query) &&
        string.IsNullOrEmpty(storefrontUri.Fragment);
}
