using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading.RateLimiting;
using System.Globalization;
using Confluent.Kafka;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using SplitPayment.Domain.Calculations;
using SplitPayment.Domain.Events;
using SplitPayment.Domain.Validation;
using SplitPayment.InvoiceApi;
using SplitPayment.InvoiceApi.Payments;

var builder = WebApplication.CreateBuilder(args);
var apiKey = builder.Configuration["Security:ApiKey"];
if (string.IsNullOrWhiteSpace(apiKey) || apiKey.Length < 32)
{
    throw new InvalidOperationException("Configure Security__ApiKey with at least 32 characters.");
}

var bootstrapServers = builder.Configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
var mongoConnection = builder.Configuration["Mongo:ConnectionString"];
if (string.IsNullOrWhiteSpace(mongoConnection))
{
    throw new InvalidOperationException("Configure Mongo__ConnectionString with authentication enabled.");
}

var mongoDatabaseName = builder.Configuration["Mongo:Database"] ?? "SplitPaymentDb";
var demoPaymentsMode = builder.Configuration["DemoPayments:Mode"] ?? DemoPaymentsOptions.Disabled;
if (demoPaymentsMode is not (DemoPaymentsOptions.Disabled or DemoPaymentsOptions.Mock))
{
    throw new InvalidOperationException("DemoPayments:Mode must be Disabled or Mock.");
}

var localMockPaymentsEnabled = demoPaymentsMode == DemoPaymentsOptions.Mock &&
    builder.Environment.IsEnvironment("LocalTesting");
if (demoPaymentsMode == DemoPaymentsOptions.Mock && !localMockPaymentsEnabled)
{
    throw new InvalidOperationException("Mock payments are available only in the LocalTesting environment.");
}

var mongoClient = new MongoClient(mongoConnection);
var outboxCollection = mongoClient.GetDatabase(mongoDatabaseName)
    .GetCollection<InvoiceOutboxDocument>("InvoiceOutbox")
    .WithWriteConcern(WriteConcern.WMajority);
var database = mongoClient.GetDatabase(mongoDatabaseName);
var checkoutCollection = database
    .GetCollection<DemoCheckoutDocument>("DemoPaymentCheckouts")
    .WithWriteConcern(WriteConcern.WMajority);
var testProfileCollection = database
    .GetCollection<DemoTestProfileDocument>("DemoTestProfiles")
    .WithWriteConcern(WriteConcern.WMajority);

builder.Services.AddSingleton<IMongoClient>(mongoClient);
builder.Services.AddSingleton(outboxCollection);
builder.Services.AddSingleton(checkoutCollection);
builder.Services.AddSingleton(testProfileCollection);
builder.Services.Configure<MercadoPagoSandboxOptions>(builder.Configuration.GetSection("MercadoPago"));
builder.Services.AddHttpClient<MercadoPagoSandboxClient>(client =>
{
    client.BaseAddress = new Uri("https://api.mercadopago.com/");
    client.Timeout = TimeSpan.FromSeconds(15);
});
builder.Services.AddSingleton<IAdminClient>(_ =>
    new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build());
builder.Services.AddSingleton<IProducer<string, string>>(_ =>
    new ProducerBuilder<string, string>(new ProducerConfig
    {
        BootstrapServers = bootstrapServers,
        Acks = Acks.All,
        EnableIdempotence = true,
        MessageTimeoutMs = 10_000,
        SocketTimeoutMs = 10_000
    }).Build());
if (!builder.Configuration.GetValue("Testing:DisableOutboxPublisher", false))
{
    builder.Services.AddHostedService<OutboxPublisher>();
}
builder.Services.AddOpenApi();
builder.Services.AddRateLimiter(options =>
{
    options.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
    options.AddPolicy("invoice-write", context => RateLimitPartition.GetFixedWindowLimiter(
        partitionKey: context.Connection.RemoteIpAddress?.ToString() ?? "unknown",
        factory: _ => new FixedWindowRateLimiterOptions
        {
            PermitLimit = 30,
            Window = TimeSpan.FromMinutes(1),
            QueueLimit = 0,
            AutoReplenishment = true
        }));
});
builder.WebHost.ConfigureKestrel(options => options.Limits.MaxRequestBodySize = 32 * 1024);

var app = builder.Build();
var jsonOptions = new JsonSerializerOptions(JsonSerializerDefaults.Web);

if (localMockPaymentsEnabled)
{
    await DemoTestProfileCatalog.SeedAsync(testProfileCollection, CancellationToken.None);
}

app.UseRateLimiter();
app.Use(async (context, next) =>
{
    if (context.Request.Path.StartsWithSegments("/api"))
    {
        var suppliedKey = context.Request.Headers["X-API-Key"].ToString();
        var suppliedBytes = Encoding.UTF8.GetBytes(suppliedKey);
        var expectedBytes = Encoding.UTF8.GetBytes(apiKey);
        if (suppliedBytes.Length != expectedBytes.Length ||
            !CryptographicOperations.FixedTimeEquals(suppliedBytes, expectedBytes))
        {
            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            await context.Response.WriteAsJsonAsync(new { error = "unauthorized" });
            return;
        }
    }

    await next();
});

app.MapOpenApi();
app.MapGet("/health/live", () => Results.Ok(new { status = "live" }));
app.MapGet("/health/ready", async (IMongoClient client, IAdminClient kafka) =>
{
    try
    {
        await client.GetDatabase("admin").RunCommandAsync<BsonDocument>(new BsonDocument("ping", 1));
        kafka.GetMetadata(TimeSpan.FromSeconds(2));
        return Results.Ok(new { status = "ready" });
    }
    catch (Exception)
    {
        return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
    }
});

app.MapPost("/api/v2/demo/tax-calculations", (TaxSimulationRequest request) =>
{
    try
    {
        var calculation = TaxSimulation2026Calculator.Calculate(request.TaxBase, request.ReferenceDate);
        return Results.Ok(calculation);
    }
    catch (ArgumentOutOfRangeException exception)
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            [exception.ParamName ?? "request"] = [exception.Message]
        });
    }
})
    .RequireRateLimiting("invoice-write")
    .WithSummary("Calculate informational 2026 IBS/CBS test values for a synthetic tax base")
    .WithDescription("Demonstration only. The caller supplies an unvalidated synthetic tax base. Does not determine fiscal classification, calculate seller settlement, issue a fiscal document, collect tax, or integrate with a payment provider.");

app.MapGet("/api/v2/demo/products", () => Results.Ok(DemoProductCatalog.GetAll()))
    .WithSummary("List fixed-price synthetic products for sandbox checkout");

app.MapGet("/api/v2/demo/test-profiles", async (
    IMongoCollection<DemoTestProfileDocument> profiles,
    CancellationToken cancellationToken) =>
{
    if (!localMockPaymentsEnabled)
    {
        return Results.NotFound();
    }

    var buyers = await profiles.Find(profile => profile.Role == "Buyer" && profile.IsSynthetic)
        .SortBy(profile => profile.Id)
        .ToListAsync(cancellationToken);
    return Results.Ok(buyers.Select(profile => new
    {
        profile.Id,
        profile.DisplayName,
        profile.Email,
        profile.IsSynthetic
    }));
})
    .WithSummary("List synthetic buyer profiles available only in local mock mode");

app.MapPost("/api/v2/demo/checkouts", async (
    CreateDemoCheckoutRequest request,
    [FromHeader(Name = "Idempotency-Key")] string? idempotencyKey,
    IMongoCollection<DemoCheckoutDocument> checkouts,
    IMongoCollection<DemoTestProfileDocument> profiles,
    MercadoPagoSandboxClient mercadoPago,
    ILogger<Program> logger,
    CancellationToken cancellationToken) =>
{
    if (!IsValidIdempotencyKey(idempotencyKey ?? string.Empty))
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["Idempotency-Key"] = ["Provide an 8-128 character ASCII idempotency key."]
        });
    }

    if (!DemoProductCatalog.TryGet(request.ProductId ?? string.Empty, out var product))
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["productId"] = ["Select a product from the synthetic demo catalog."]
        });
    }

    if (request.Quantity is < 1 or > 10)
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["quantity"] = ["Quantity must be between 1 and 10."]
        });
    }

    DemoTestProfileDocument? testProfile = null;
    if (localMockPaymentsEnabled)
    {
        if (string.IsNullOrWhiteSpace(request.TestProfileId))
        {
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                ["testProfileId"] = ["Select a synthetic buyer profile for local mock checkout."]
            });
        }

        testProfile = await profiles.Find(profile =>
                profile.Id == request.TestProfileId &&
                profile.Role == "Buyer" &&
                profile.IsSynthetic)
            .FirstOrDefaultAsync(cancellationToken);
        if (testProfile is null)
        {
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                ["testProfileId"] = ["Select an active synthetic buyer profile."]
            });
        }
    }
    else if (request.TestProfileId is not null)
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["testProfileId"] = ["Test profiles are only available in local mock mode."]
        });
    }

    if (!localMockPaymentsEnabled && !mercadoPago.IsEnabled)
    {
        return Results.Problem(
            title: "Sandbox checkout is not configured",
            detail: "Configure Mercado Pago TEST credentials, webhook secret, and an HTTPS notification URL.",
            statusCode: StatusCodes.Status503ServiceUnavailable);
    }

    var checkoutId = Guid.NewGuid().ToString("D");
    var totalAmount = product.UnitPrice * request.Quantity;
    var requestHash = Convert.ToHexString(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(new
    {
        product.Id,
        request.Quantity,
        product.UnitPrice,
        testProfileId = testProfile?.Id
    }, jsonOptions)));
    var checkout = new DemoCheckoutDocument
    {
        IdempotencyKey = idempotencyKey!,
        CheckoutId = checkoutId,
        RequestHash = requestHash,
        ProductId = product.Id,
        ProductName = product.Name,
        UnitPrice = product.UnitPrice,
        Quantity = request.Quantity,
        TotalAmount = totalAmount,
        Status = localMockPaymentsEnabled ? "pending" : "Creating",
        IsMockPayment = localMockPaymentsEnabled,
        TestProfileId = testProfile?.Id,
        TestProfileName = testProfile?.DisplayName,
        MockCheckoutUrl = localMockPaymentsEnabled ? $"/checkout/mock/{checkoutId}" : null,
        CreatedAt = DateTime.UtcNow,
        UpdatedAt = DateTime.UtcNow
    };

    try
    {
        await checkouts.InsertOneAsync(checkout, cancellationToken: cancellationToken);
    }
    catch (MongoWriteException exception) when (exception.WriteError.Category == ServerErrorCategory.DuplicateKey)
    {
        var existing = await checkouts.Find(item => item.IdempotencyKey == idempotencyKey)
            .FirstOrDefaultAsync(cancellationToken);
        if (existing is null)
        {
            throw;
        }

        if (!CryptographicOperations.FixedTimeEquals(
                Convert.FromHexString(existing.RequestHash),
                Convert.FromHexString(requestHash)))
        {
            return Results.Conflict(new { error = "idempotency_key_reused_with_different_checkout" });
        }

        return existing.IsMockPayment && existing.MockCheckoutUrl is not null ||
               existing.Status == "Created" && existing.SandboxCheckoutUrl is not null
            ? Results.Ok(ToCheckoutResponse(existing))
            : Results.Conflict(new { error = "checkout_creation_in_progress_or_provider_outcome_unknown" });
    }

    if (localMockPaymentsEnabled)
    {
        return Results.Created($"/api/v2/demo/checkouts/{checkoutId}", ToCheckoutResponse(checkout));
    }

    try
    {
        var preference = await mercadoPago.CreatePreferenceAsync(checkout, cancellationToken);
        var update = Builders<DemoCheckoutDocument>.Update
            .Set(item => item.Status, "Created")
            .Set(item => item.ProviderPreferenceId, preference.Id)
            .Set(item => item.SandboxCheckoutUrl, preference.SandboxInitPoint)
            .Set(item => item.UpdatedAt, DateTime.UtcNow);
        var updateResult = await checkouts.UpdateOneAsync(
            item => item.IdempotencyKey == idempotencyKey && item.Status == "Creating",
            update,
            cancellationToken: cancellationToken);
        if (updateResult.MatchedCount == 0)
        {
            await checkouts.UpdateOneAsync(
                item => item.IdempotencyKey == idempotencyKey,
                Builders<DemoCheckoutDocument>.Update
                    .Set(item => item.ProviderPreferenceId, preference.Id)
                    .Set(item => item.SandboxCheckoutUrl, preference.SandboxInitPoint)
                    .Set(item => item.UpdatedAt, DateTime.UtcNow),
                cancellationToken: cancellationToken);
        }

        var savedCheckout = await checkouts.Find(item => item.IdempotencyKey == idempotencyKey)
            .FirstOrDefaultAsync(cancellationToken);
        if (savedCheckout is null)
        {
            throw new InvalidOperationException("The sandbox checkout disappeared after provider creation.");
        }

        return Results.Created($"/api/v2/demo/checkouts/{checkoutId}", ToCheckoutResponse(savedCheckout));
    }
    catch (MercadoPagoProviderException exception)
    {
        logger.LogWarning("Mercado Pago sandbox preference creation failed with HTTP {StatusCode}.", (int)exception.StatusCode);
        await checkouts.UpdateOneAsync(
            item => item.IdempotencyKey == idempotencyKey && item.Status == "Creating",
            Builders<DemoCheckoutDocument>.Update
                .Set(item => item.Status, "Failed")
                .Set(item => item.UpdatedAt, DateTime.UtcNow),
            cancellationToken: cancellationToken);
        return Results.Problem(
            title: "Sandbox checkout could not be created",
            detail: "Mercado Pago did not accept the sandbox checkout request.",
            statusCode: StatusCodes.Status502BadGateway);
    }
    catch (HttpRequestException)
    {
        await checkouts.UpdateOneAsync(
            item => item.IdempotencyKey == idempotencyKey && item.Status == "Creating",
            Builders<DemoCheckoutDocument>.Update
                .Set(item => item.Status, "ProviderOutcomeUnknown")
                .Set(item => item.UpdatedAt, DateTime.UtcNow),
            cancellationToken: cancellationToken);
        return Results.Problem(
            title: "Sandbox checkout outcome is unknown",
            detail: "Check the payment provider before retrying with a new idempotency key.",
            statusCode: StatusCodes.Status502BadGateway);
    }
    catch (InvalidDataException)
    {
        await checkouts.UpdateOneAsync(
            item => item.IdempotencyKey == idempotencyKey && item.Status == "Creating",
            Builders<DemoCheckoutDocument>.Update
                .Set(item => item.Status, "ProviderOutcomeUnknown")
                .Set(item => item.UpdatedAt, DateTime.UtcNow),
            cancellationToken: cancellationToken);
        return Results.Problem(
            title: "Sandbox checkout outcome is unknown",
            detail: "Check the payment provider before retrying with a new idempotency key.",
            statusCode: StatusCodes.Status502BadGateway);
    }
    catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
    {
        await checkouts.UpdateOneAsync(
            item => item.IdempotencyKey == idempotencyKey && item.Status == "Creating",
            Builders<DemoCheckoutDocument>.Update
                .Set(item => item.Status, "ProviderOutcomeUnknown")
                .Set(item => item.UpdatedAt, DateTime.UtcNow),
            cancellationToken: CancellationToken.None);
        return Results.Problem(
            title: "Sandbox checkout outcome is unknown",
            detail: "Check the payment provider before retrying with a new idempotency key.",
            statusCode: StatusCodes.Status502BadGateway);
    }
})
    .RequireRateLimiting("invoice-write")
    .WithSummary("Create an idempotent synthetic checkout in local mock or provider sandbox mode")
    .WithDescription("Local mock mode is restricted to the LocalTesting environment. Provider checkout uses Mercado Pago TEST credentials and never receives card numbers or CVV in this application.");

app.MapPost("/api/v2/demo/checkouts/{checkoutId:guid}/mock-payment", async (
    Guid checkoutId,
    CompleteMockPaymentRequest request,
    IMongoCollection<DemoCheckoutDocument> checkouts,
    CancellationToken cancellationToken) =>
{
    if (!localMockPaymentsEnabled)
    {
        return Results.NotFound();
    }

    if (request.Status is not ("approved" or "rejected" or "pending"))
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["status"] = ["Select approved, rejected, or pending for the local simulation."]
        });
    }

    var checkoutFilter = Builders<DemoCheckoutDocument>.Filter.Eq(
            item => item.CheckoutId,
            checkoutId.ToString("D")) &
        Builders<DemoCheckoutDocument>.Filter.Eq(item => item.IsMockPayment, true);
    var updated = await checkouts.UpdateOneAsync(
        checkoutFilter & Builders<DemoCheckoutDocument>.Filter.Eq(item => item.Status, "pending"),
        Builders<DemoCheckoutDocument>.Update
            .Set(item => item.Status, request.Status)
            .Set(item => item.UpdatedAt, DateTime.UtcNow),
        cancellationToken: cancellationToken);

    if (updated.ModifiedCount == 0)
    {
        var existing = await checkouts.Find(checkoutFilter).FirstOrDefaultAsync(cancellationToken);
        if (existing is null)
        {
            return Results.NotFound();
        }

        if (existing.Status != request.Status)
        {
            return Results.Conflict(new { error = "mock_payment_already_has_a_different_final_status" });
        }
    }

    return Results.Ok();
})
    .RequireRateLimiting("invoice-write")
    .WithSummary("Set a local mock payment result without contacting a payment provider");

app.MapGet("/api/v2/demo/checkouts/{checkoutId:guid}", async (
    Guid checkoutId,
    IMongoCollection<DemoCheckoutDocument> checkouts,
    CancellationToken cancellationToken) =>
{
    var checkout = await checkouts.Find(item => item.CheckoutId == checkoutId.ToString("D"))
        .FirstOrDefaultAsync(cancellationToken);
    return checkout is null
        ? Results.NotFound()
        : Results.Ok(new
        {
            checkout.CheckoutId,
            checkout.ProductId,
            checkout.ProductName,
            checkout.UnitPrice,
            checkout.Quantity,
            checkout.TotalAmount,
            checkout.Status,
            checkout.ProviderPaymentId,
            checkout.TestProfileName,
            checkout.CreatedAt,
            checkout.UpdatedAt,
            isSandbox = !checkout.IsMockPayment,
            checkout.IsMockPayment
        });
})
    .WithSummary("Get demo checkout status");

app.MapPost("/webhooks/mercadopago/payments", async (
    HttpRequest request,
    IMongoCollection<DemoCheckoutDocument> checkouts,
    MercadoPagoSandboxClient mercadoPago,
    IOptions<MercadoPagoSandboxOptions> options,
    ILogger<Program> logger,
    CancellationToken cancellationToken) =>
{
    var settings = options.Value;
    var dataId = request.Query["data.id"].ToString();
    var requestId = request.Headers["x-request-id"].ToString();
    if (!mercadoPago.IsEnabled ||
        !MercadoPagoWebhookSignatureValidator.IsValid(
            request.Headers["x-signature"],
            requestId,
            dataId,
            settings.WebhookSecret,
            DateTimeOffset.UtcNow))
    {
        return Results.Unauthorized();
    }

    JsonDocument notification;
    try
    {
        notification = await JsonDocument.ParseAsync(request.Body, cancellationToken: cancellationToken);
    }
    catch (JsonException)
    {
        return Results.BadRequest();
    }

    using (notification)
    {
        var root = notification.RootElement;
        if (!root.TryGetProperty("type", out var typeElement) ||
            typeElement.ValueKind != JsonValueKind.String ||
            !string.Equals(typeElement.GetString(), "payment", StringComparison.Ordinal) ||
            !root.TryGetProperty("data", out var dataElement) ||
            dataElement.ValueKind != JsonValueKind.Object ||
            !dataElement.TryGetProperty("id", out var idElement) ||
            !string.Equals(idElement.ToString(), dataId, StringComparison.Ordinal) ||
            !root.TryGetProperty("live_mode", out var liveModeElement) ||
            liveModeElement.ValueKind != JsonValueKind.False)
        {
            return Results.BadRequest();
        }
    }

    try
    {
        var payment = await mercadoPago.GetPaymentAsync(dataId, cancellationToken);
        if (!Guid.TryParse(payment.ExternalReference, out var checkoutId))
        {
            logger.LogWarning("Ignoring sandbox payment with an unrecognized external reference.");
            return Results.Ok();
        }

        var filter = Builders<DemoCheckoutDocument>.Filter.Eq(
            item => item.CheckoutId,
            checkoutId.ToString("D"));
        var checkout = await checkouts.Find(filter).FirstOrDefaultAsync(cancellationToken);
        if (checkout is null)
        {
            logger.LogWarning("Ignoring sandbox payment for an unknown checkout.");
            return Results.Ok();
        }

        if (payment.TransactionAmount != checkout.TotalAmount ||
            !IsKnownProviderPaymentStatus(payment.Status))
        {
            logger.LogWarning("Sandbox payment amount or status did not match checkout {CheckoutId}.", checkoutId);
            return Results.BadRequest();
        }

        await checkouts.UpdateOneAsync(
            filter,
            Builders<DemoCheckoutDocument>.Update
                .Set(item => item.Status, payment.Status)
                .Set(item => item.ProviderPaymentId, payment.Id)
                .Set(item => item.UpdatedAt, DateTime.UtcNow),
            cancellationToken: cancellationToken);
        return Results.Ok();
    }
    catch (MercadoPagoProviderException exception)
    {
        logger.LogWarning("Mercado Pago sandbox payment lookup failed with HTTP {StatusCode}.", (int)exception.StatusCode);
        return Results.StatusCode(StatusCodes.Status502BadGateway);
    }
    catch (HttpRequestException)
    {
        return Results.StatusCode(StatusCodes.Status502BadGateway);
    }
    catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
    {
        return Results.StatusCode(StatusCodes.Status502BadGateway);
    }
})
    .WithSummary("Validate a Mercado Pago payment webhook and refresh payment status");

app.MapPost("/api/v1/invoices", async (
    CreateInvoiceRequest request,
    [FromHeader(Name = "X-API-Key")] string apiKeyHeader,
    [FromHeader(Name = "Idempotency-Key")] string? idempotencyKey,
    IMongoCollection<InvoiceOutboxDocument> outbox,
    CancellationToken cancellationToken) =>
{
    var idempotencyKeyValue = idempotencyKey ?? string.Empty;
    if (!IsValidIdempotencyKey(idempotencyKeyValue))
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["Idempotency-Key"] = ["Provide a key of 8 to 128 ASCII letters, digits, '.', '_' or '-'."]
        });
    }

    var errors = ValidateRequest(request);
    if (errors.Count > 0)
    {
        return Results.ValidationProblem(errors);
    }

    var invoice = new InvoiceCreatedEvent(
        Guid.NewGuid(),
        TaxDocumentValidator.Normalize(request.MerchantTaxId!),
        TaxDocumentValidator.Normalize(request.CustomerTaxId!),
        request.GrossAmount,
        request.IbsRate,
        request.CbsRate,
        request.IsSimplifiedProcedure,
        DateTime.UtcNow);

    try
    {
        SplitCalculator.Calculate(invoice);
        var canonicalRequest = JsonSerializer.SerializeToUtf8Bytes(new
        {
            invoice.MerchantTaxId,
            invoice.CustomerTaxId,
            grossAmount = invoice.GrossAmount.ToString("G29", CultureInfo.InvariantCulture),
            ibsRate = invoice.IbsRate.ToString("G29", CultureInfo.InvariantCulture),
            cbsRate = invoice.CbsRate.ToString("G29", CultureInfo.InvariantCulture),
            invoice.IsSimplifiedProcedure
        }, jsonOptions);
        var requestHash = Convert.ToHexString(SHA256.HashData(canonicalRequest));
        var document = new InvoiceOutboxDocument
        {
            Id = idempotencyKeyValue,
            RequestHash = requestHash,
            InvoiceId = invoice.InvoiceId.ToString("D"),
            EventJson = JsonSerializer.Serialize(invoice, jsonOptions),
            Status = "Pending",
            CreatedAt = DateTime.UtcNow
        };

        try
        {
            await outbox.InsertOneAsync(document, cancellationToken: cancellationToken);
        }
        catch (MongoWriteException exception) when (exception.WriteError.Category == ServerErrorCategory.DuplicateKey)
        {
            document = await outbox.Find(item => item.Id == idempotencyKeyValue)
                .FirstOrDefaultAsync(cancellationToken);
            if (document is null)
            {
                throw;
            }

            if (!CryptographicOperations.FixedTimeEquals(
                    Convert.FromHexString(document.RequestHash),
                    Convert.FromHexString(requestHash)))
            {
                return Results.Conflict(new { error = "idempotency_key_reused_with_different_request" });
            }
        }

        return Results.Accepted($"/api/v1/invoices/{document.InvoiceId}", new
        {
            invoiceId = document.InvoiceId,
            status = document.Status
        });
    }
    catch (ArgumentOutOfRangeException exception)
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["request"] = [exception.Message]
        });
    }
    catch (MongoException)
    {
        return Results.Problem(
            title: "Invoice intake unavailable",
            detail: "The invoice could not be durably accepted.",
            statusCode: StatusCodes.Status503ServiceUnavailable);
    }
})
    .RequireRateLimiting("invoice-write")
    .WithSummary("Accept a legacy illustrative invoice for asynchronous processing")
    .WithDescription("Legacy sandbox contract using caller-supplied illustrative rates; it is not the 2026 IBS/CBS informational calculation and must not be used for fiscal accounting. Requires X-API-Key and Idempotency-Key headers.");

app.MapGet("/api/v1/invoices/{invoiceId:guid}", async (
    Guid invoiceId,
    [FromHeader(Name = "X-API-Key")] string apiKeyHeader,
    IMongoCollection<InvoiceOutboxDocument> outbox,
    CancellationToken cancellationToken) =>
{
    var document = await outbox.Find(item => item.InvoiceId == invoiceId.ToString("D"))
        .FirstOrDefaultAsync(cancellationToken);
    return document is null
        ? Results.NotFound()
        : Results.Ok(new { invoiceId, status = document.Status, createdAt = document.CreatedAt, publishedAt = document.PublishedAt });
})
    .WithSummary("Get invoice publishing status");

app.Run();

static object ToCheckoutResponse(DemoCheckoutDocument checkout) => new
{
    checkout.CheckoutId,
    checkout.ProductId,
    checkout.ProductName,
    checkout.UnitPrice,
    checkout.Quantity,
    checkout.TotalAmount,
    checkout.Status,
    checkout.ProviderPreferenceId,
    checkout.SandboxCheckoutUrl,
    checkout.MockCheckoutUrl,
    checkout.TestProfileName,
    checkout.IsMockPayment,
    isSandbox = !checkout.IsMockPayment
};

static bool IsKnownProviderPaymentStatus(string status) =>
    status is "approved" or "pending" or "in_process" or "rejected" or "cancelled" or "refunded" or "charged_back";

static bool IsValidIdempotencyKey(string value) =>
    value.Length is >= 8 and <= 128 &&
    value.All(character => char.IsAsciiLetterOrDigit(character) || character is '.' or '_' or '-');

static Dictionary<string, string[]> ValidateRequest(CreateInvoiceRequest request)
{
    var errors = new Dictionary<string, string[]>();

    if (!TaxDocumentValidator.IsValid(request.MerchantTaxId) ||
        TaxDocumentValidator.Normalize(request.MerchantTaxId!).Length != 14)
    {
        errors["merchantTaxId"] = ["A valid CNPJ is required for the merchant."];
    }

    if (!TaxDocumentValidator.IsValid(request.CustomerTaxId))
    {
        errors["customerTaxId"] = ["A valid CPF or CNPJ is required for the customer."];
    }

    return errors;
}

public sealed record CreateInvoiceRequest(
    string? MerchantTaxId,
    string? CustomerTaxId,
    decimal GrossAmount,
    decimal IbsRate,
    decimal CbsRate,
    bool IsSimplifiedProcedure);

public sealed record TaxSimulationRequest(decimal TaxBase, DateOnly ReferenceDate);

public sealed record CreateDemoCheckoutRequest(string? ProductId, int Quantity, string? TestProfileId = null);

public sealed record CompleteMockPaymentRequest(string Status);

public partial class Program { }
