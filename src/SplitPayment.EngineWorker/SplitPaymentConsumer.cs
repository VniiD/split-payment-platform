using System.Text.Json;
using Confluent.Kafka;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using SplitPayment.Domain.Calculations;
using SplitPayment.Domain.Events;

namespace SplitPayment.EngineWorker;

public sealed class SplitPaymentConsumer : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly ILogger<SplitPaymentConsumer> _logger;
    private readonly IMongoCollection<SplitTransactionDocument> _transactions;
    private readonly IConfiguration _configuration;

    public SplitPaymentConsumer(
        ILogger<SplitPaymentConsumer> logger,
        IMongoClient mongoClient,
        IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        var databaseName = configuration["Mongo:Database"] ?? "SplitPaymentDb";
        _transactions = mongoClient.GetDatabase(databaseName)
            .GetCollection<SplitTransactionDocument>("Transactions");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var bootstrapServers = _configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
        var topic = _configuration["Kafka:InvoiceTopic"] ?? "invoice-created-topic";
        var groupId = _configuration["Kafka:ConsumerGroup"] ?? "split-payment-engine-group";
        var deadLetterTopic = _configuration["Kafka:DeadLetterTopic"] ?? "invoice-created-engine-dlq";
        var maxAttempts = Math.Max(1, _configuration.GetValue("Processing:MaxAttempts", 3));
        await _transactions.Indexes.CreateOneAsync(
            new CreateIndexModel<SplitTransactionDocument>(
                Builders<SplitTransactionDocument>.IndexKeys.Ascending(document => document.InvoiceId),
                new CreateIndexOptions { Unique = true, Name = "UQ_Transactions_InvoiceId" }),
            cancellationToken: stoppingToken);

        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            AllowAutoCreateTopics = false
        }).Build();
        using var deadLetterProducer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true
        }).Build();

        consumer.Subscribe(topic);
        _logger.LogInformation("Consuming {Topic} as {GroupId}", topic, groupId);

        while (!stoppingToken.IsCancellationRequested)
        {
            ConsumeResult<string, string>? result = null;
            try
            {
                result = consumer.Consume(stoppingToken);
                if (result?.Message?.Value is null)
                {
                    continue;
                }

                if (await ProcessWithRetriesAsync(result, deadLetterProducer, deadLetterTopic, maxAttempts, stoppingToken))
                {
                    consumer.Commit(result);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                _logger.LogError(
                    "Consumer processing failed with {ErrorCode}; offset remains uncommitted.",
                    exception.GetType().Name);
                if (result is not null)
                {
                    consumer.Seek(result.TopicPartitionOffset);
                }

                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
        }

        consumer.Close();
    }

    private async Task<bool> ProcessWithRetriesAsync(
        ConsumeResult<string, string> result,
        IProducer<string, string> deadLetterProducer,
        string deadLetterTopic,
        int maxAttempts,
        CancellationToken cancellationToken)
    {
        Exception? lastException = null;

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                var invoice = JsonSerializer.Deserialize<InvoiceCreatedEvent>(result.Message.Value, JsonOptions)
                    ?? throw new JsonException("Invoice event payload is empty.");
                if (invoice.InvoiceId == Guid.Empty)
                {
                    throw new JsonException("InvoiceId is required.");
                }

                if (invoice.SchemaVersion != InvoiceCreatedEvent.CurrentSchemaVersion)
                {
                    throw new JsonException($"Unsupported invoice event schema version: {invoice.SchemaVersion}.");
                }

                var calculation = SplitCalculator.Calculate(invoice);
                var document = new SplitTransactionDocument
                {
                    Id = invoice.InvoiceId.ToString("D"),
                    InvoiceId = invoice.InvoiceId,
                    MerchantTaxId = invoice.MerchantTaxId,
                    GrossValue = invoice.GrossAmount,
                    IbsDeduction = calculation.IbsAmount,
                    CbsDeduction = calculation.CbsAmount,
                    NetMerchantValue = calculation.NetAmount,
                    ProcessedAt = DateTime.UtcNow
                };

                try
                {
                    await _transactions.InsertOneAsync(document, cancellationToken: cancellationToken);
                }
                catch (MongoWriteException exception) when (
                    exception.WriteError.Category == ServerErrorCategory.DuplicateKey)
                {
                    _logger.LogInformation("Invoice {InvoiceId} already exists in MongoDB.", invoice.InvoiceId);
                }

                _logger.LogInformation("Split persisted for invoice {InvoiceId}.", invoice.InvoiceId);
                return true;
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                lastException = exception;
                _logger.LogWarning(exception, "Attempt {Attempt}/{MaxAttempts} failed.", attempt, maxAttempts);
                if (attempt < maxAttempts)
                {
                    await Task.Delay(TimeSpan.FromSeconds(attempt), cancellationToken);
                }
            }
        }

        var deadLetter = JsonSerializer.Serialize(new
        {
            result.Topic,
            result.Partition,
            result.Offset,
            result.Message.Key,
            result.Message.Value,
            FailureCode = lastException?.GetType().Name,
            FailedAt = DateTime.UtcNow
        }, JsonOptions);

        await deadLetterProducer.ProduceAsync(deadLetterTopic, new Message<string, string>
        {
            Key = result.Message.Key,
            Value = deadLetter
        }, cancellationToken);

        _logger.LogError(lastException, "Invoice event sent to {DeadLetterTopic}.", deadLetterTopic);
        return true;
    }
}

public sealed class SplitTransactionDocument
{
    [BsonId]
    public string Id { get; set; } = string.Empty;
    [BsonElement("invoiceId")]
    [BsonRepresentation(BsonType.String)]
    public Guid InvoiceId { get; set; }
    [BsonElement("merchantTaxId")]
    public string MerchantTaxId { get; set; } = string.Empty;
    [BsonElement("grossValue")]
    [BsonRepresentation(BsonType.Decimal128)]
    public decimal GrossValue { get; set; }
    [BsonElement("ibsDeduction")]
    [BsonRepresentation(BsonType.Decimal128)]
    public decimal IbsDeduction { get; set; }
    [BsonElement("cbsDeduction")]
    [BsonRepresentation(BsonType.Decimal128)]
    public decimal CbsDeduction { get; set; }
    [BsonElement("netMerchantValue")]
    [BsonRepresentation(BsonType.Decimal128)]
    public decimal NetMerchantValue { get; set; }
    [BsonElement("processedAt")]
    public DateTime ProcessedAt { get; set; }
}