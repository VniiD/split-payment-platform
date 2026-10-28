using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading.RateLimiting;
using System.Globalization;
using Confluent.Kafka;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.AspNetCore.Mvc;
using MongoDB.Bson;
using MongoDB.Driver;
using SplitPayment.Domain.Calculations;
using SplitPayment.Domain.Events;
using SplitPayment.Domain.Validation;
using SplitPayment.InvoiceApi;

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
var mongoClient = new MongoClient(mongoConnection);
var outboxCollection = mongoClient.GetDatabase(mongoDatabaseName)
    .GetCollection<InvoiceOutboxDocument>("InvoiceOutbox")
    .WithWriteConcern(WriteConcern.WMajority);

builder.Services.AddSingleton<IMongoClient>(mongoClient);
builder.Services.AddSingleton(outboxCollection);
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

app.UseRateLimiter();
app.Use(async (context, next) =>
{
    if (context.Request.Path.StartsWithSegments("/api/v1"))
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
    .WithSummary("Accept an invoice for asynchronous processing")
    .WithDescription("Requires X-API-Key and Idempotency-Key headers. A 202 response means the event was durably stored in the MongoDB outbox.");

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

public partial class Program { }
