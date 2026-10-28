using System.Data;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Data.SqlClient;
using SplitPayment.Domain.Calculations;
using SplitPayment.Domain.Events;

namespace SplitPayment.CgibsGateway;

public sealed class CgibsLedgerConsumer : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly ILogger<CgibsLedgerConsumer> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _ledgerConnectionString;

    public CgibsLedgerConsumer(ILogger<CgibsLedgerConsumer> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        var configuredConnection = configuration["Sql:ConnectionString"];
        if (string.IsNullOrWhiteSpace(configuredConnection))
        {
            throw new InvalidOperationException("Configure Sql__ConnectionString using an environment variable or user secret.");
        }

        _ledgerConnectionString = new SqlConnectionStringBuilder(configuredConnection).ConnectionString;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var bootstrapServers = _configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
        var topic = _configuration["Kafka:InvoiceTopic"] ?? "invoice-created-topic";
        var groupId = _configuration["Kafka:ConsumerGroup"] ?? "cgibs-gateway-group";
        var deadLetterTopic = _configuration["Kafka:DeadLetterTopic"] ?? "invoice-created-cgibs-dlq";
        var maxAttempts = Math.Max(1, _configuration.GetValue("Processing:MaxAttempts", 3));

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
                await InsertLedgerRowAsync(invoice, calculation, cancellationToken);
                _logger.LogInformation("Ledger row persisted for invoice {InvoiceId}.", invoice.InvoiceId);
                return true;
            }
            catch (SqlException exception) when (exception.Number is 2601 or 2627)
            {
                _logger.LogInformation("Invoice {InvoiceId} already exists in SQL Server.", result.Message.Key);
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

    private async Task InsertLedgerRowAsync(
        InvoiceCreatedEvent invoice,
        SplitCalculation calculation,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(_ledgerConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO dbo.CgibsTaxLedger
                (LedgerId, InvoiceId, MerchantTaxId, GrossAmount, IbsAmount, CbsAmount, NetAmount, ProcessedAt, Status)
            VALUES
                (@LedgerId, @InvoiceId, @MerchantTaxId, @GrossAmount, @IbsAmount, @CbsAmount, @NetAmount, @ProcessedAt, @Status);
            """;
        command.Parameters.Add("@LedgerId", SqlDbType.UniqueIdentifier).Value = Guid.NewGuid();
        command.Parameters.Add("@InvoiceId", SqlDbType.UniqueIdentifier).Value = invoice.InvoiceId;
        command.Parameters.Add("@MerchantTaxId", SqlDbType.VarChar, 14).Value = invoice.MerchantTaxId;
        AddDecimal(command, "@GrossAmount", invoice.GrossAmount, 18, 4);
        AddDecimal(command, "@IbsAmount", calculation.IbsAmount, 28, 12);
        AddDecimal(command, "@CbsAmount", calculation.CbsAmount, 28, 12);
        AddDecimal(command, "@NetAmount", calculation.NetAmount, 28, 12);
        command.Parameters.Add("@ProcessedAt", SqlDbType.DateTime2).Value = DateTime.UtcNow;
        command.Parameters.Add("@Status", SqlDbType.VarChar, 20).Value = "Retained";
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void AddDecimal(SqlCommand command, string name, decimal value, byte precision, byte scale)
    {
        var parameter = command.Parameters.Add(name, SqlDbType.Decimal);
        parameter.Precision = precision;
        parameter.Scale = scale;
        parameter.Value = value;
    }
}