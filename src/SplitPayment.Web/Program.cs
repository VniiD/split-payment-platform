using SplitPayment.Web.Components;
using SplitPayment.Web.Services;

var builder = WebApplication.CreateBuilder(args);
var backendBaseUrl = builder.Configuration["Backend:BaseUrl"] ?? "http://localhost:5080/";
var backendApiKey = builder.Configuration["Backend:ApiKey"];
if (!Uri.TryCreate(backendBaseUrl, UriKind.Absolute, out var backendUri) ||
    backendUri.Scheme is not ("http" or "https") ||
    !string.IsNullOrEmpty(backendUri.UserInfo))
{
    throw new InvalidOperationException("Configure Backend__BaseUrl with a valid HTTP or HTTPS API URL.");
}

if (string.IsNullOrWhiteSpace(backendApiKey) || backendApiKey.Length < 32)
{
    throw new InvalidOperationException("Configure Backend__ApiKey with at least 32 characters.");
}

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();
builder.Services.AddHttpClient<DemoCheckoutApi>(client =>
{
    client.BaseAddress = backendUri;
    client.Timeout = TimeSpan.FromSeconds(15);
    client.DefaultRequestHeaders.Add("X-API-Key", backendApiKey);
});

var app = builder.Build();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    app.UseHsts();
    app.UseHttpsRedirection();
}
app.UseStatusCodePagesWithReExecute("/not-found", createScopeForStatusCodePages: true);
app.UseAntiforgery();
app.Use(async (context, next) =>
{
    context.Response.Headers.XContentTypeOptions = "nosniff";
    context.Response.Headers.XFrameOptions = "DENY";
    context.Response.Headers["Referrer-Policy"] = "strict-origin-when-cross-origin";
    context.Response.Headers.ContentSecurityPolicy =
        "default-src 'self'; base-uri 'self'; object-src 'none'; frame-ancestors 'none'; " +
        "img-src 'self' data:; style-src 'self'; script-src 'self'; connect-src 'self' ws: wss:";
    await next();
});

app.MapStaticAssets();
app.MapGet("/health/live", () => Results.Ok(new { status = "live" }));
app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();

public partial class Program { }
