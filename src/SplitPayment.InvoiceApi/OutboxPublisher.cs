using System.Text.Json;
using Confluent.Kafka;
using MongoDB.Driver;
using SplitPayment.Domain.Events;

namespace SplitPayment.InvoiceApi;

public sealed class OutboxPublisher : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly ILogger<OutboxPublisher> _logger;
    private readonly IMongoCollection<InvoiceOutboxDocument> _outbox;
    private readonly IProducer<string, string> _producer;
    private readonly IConfiguration _configuration;

    public OutboxPublisher(
        ILogger<OutboxPublisher> logger,
        IMongoCollection<InvoiceOutboxDocument> outbox,
        IProducer<string, string> producer,
        IConfiguration configuration)
    {
        _logger = logger;
        _outbox = outbox;
        _producer = producer;
        _configuration = configuration;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var topic = _configuration["Kafka:InvoiceTopic"] ?? "invoice-created-topic";
        var maxAttempts = Math.Max(1, _configuration.GetValue("Outbox:MaxAttempts", 12));
        await _outbox.Indexes.CreateOneAsync(
            new CreateIndexModel<InvoiceOutboxDocument>(
                Builders<InvoiceOutboxDocument>.IndexKeys.Ascending(document => document.Status)
                    .Ascending(document => document.CreatedAt),
                new CreateIndexOptions { Name = "IX_InvoiceOutbox_Status_CreatedAt" }),
            cancellationToken: stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var now = DateTime.UtcNow;
                var available = Builders<InvoiceOutboxDocument>.Filter.Or(
                    Builders<InvoiceOutboxDocument>.Filter.Eq(document => document.Status, "Pending"),
                    Builders<InvoiceOutboxDocument>.Filter.And(
                        Builders<InvoiceOutboxDocument>.Filter.Eq(document => document.Status, "Publishing"),
                        Builders<InvoiceOutboxDocument>.Filter.Lte(document => document.LeaseUntil, now)));
                var pending = await _outbox.FindOneAndUpdateAsync(
                    available,
                    Builders<InvoiceOutboxDocument>.Update
                        .Set(document => document.Status, "Publishing")
                        .Set(document => document.LeaseUntil, now.AddMinutes(1))
                        .Inc(document => document.Attempts, 1),
                    new FindOneAndUpdateOptions<InvoiceOutboxDocument>
                    {
                        Sort = Builders<InvoiceOutboxDocument>.Sort.Ascending(document => document.CreatedAt),
                        ReturnDocument = ReturnDocument.After
                    },
                    stoppingToken);

                if (pending is null)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(500), stoppingToken);
                    continue;
                }

                try
                {
                    var invoiceEvent = JsonSerializer.Deserialize<InvoiceCreatedEvent>(pending.EventJson, JsonOptions)
                        ?? throw new JsonException("Outbox event payload is empty.");
                    await _producer.ProduceAsync(topic, new Message<string, string>
                    {
                        Key = invoiceEvent.MerchantTaxId,
                        Value = pending.EventJson
                    }, stoppingToken);

                    await _outbox.UpdateOneAsync(
                        document => document.Id == pending.Id && document.Status == "Publishing",
                        Builders<InvoiceOutboxDocument>.Update
                            .Set(document => document.Status, "Published")
                            .Set(document => document.PublishedAt, DateTime.UtcNow)
                            .Unset(document => document.LeaseUntil)
                            .Unset(document => document.LastErrorCode),
                        cancellationToken: stoppingToken);
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    await _outbox.UpdateOneAsync(
                        document => document.Id == pending.Id && document.Status == "Publishing",
                        Builders<InvoiceOutboxDocument>.Update
                            .Set(document => document.Status, pending.Attempts >= maxAttempts ? "Failed" : "Pending")
                            .Unset(document => document.LeaseUntil)
                            .Set(document => document.LastErrorCode, exception.GetType().Name),
                        cancellationToken: stoppingToken);

                    if (pending.Attempts >= maxAttempts)
                    {
                        _logger.LogError(
                            "Outbox item {OutboxId} marked Failed after {Attempt} attempts ({ErrorCode}).",
                            pending.Id,
                            pending.Attempts,
                            exception.GetType().Name);
                    }
                    else
                    {
                        _logger.LogWarning(
                            "Outbox publish attempt {Attempt}/{MaxAttempts} failed with {ErrorCode}.",
                            pending.Attempts,
                            maxAttempts,
                            exception.GetType().Name);
                        await Task.Delay(TimeSpan.FromSeconds(Math.Min(30, Math.Max(1, pending.Attempts))), stoppingToken);
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                _logger.LogError("Outbox poll failed with {ErrorCode}.", exception.GetType().Name);
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
        }
    }
}