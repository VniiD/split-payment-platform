using System.Data;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using Microsoft.Data.SqlClient;

namespace SplitPayment.CgibsGateway;

public sealed class ReconciliationWorker : BackgroundService
{
    private readonly ILogger<ReconciliationWorker> _logger;
    private readonly IConfiguration _configuration;
    private readonly IMongoCollection<ReconciliationDocument> _transactions;
    private readonly string _connectionString;

    public ReconciliationWorker(
        ILogger<ReconciliationWorker> logger,
        IConfiguration configuration,
        IMongoClient mongoClient)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = configuration["Sql:ConnectionString"]
            ?? throw new InvalidOperationException("Configure Sql__ConnectionString using an environment variable or user secret.");
        var databaseName = configuration["Mongo:Database"] ?? "SplitPaymentDb";
        _transactions = mongoClient.GetDatabase(databaseName)
            .GetCollection<ReconciliationDocument>("Transactions");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var intervalSeconds = Math.Max(30, _configuration.GetValue("Reconciliation:IntervalSeconds", 300));
        var stabilityWindowSeconds = Math.Max(0, _configuration.GetValue("Reconciliation:StabilityWindowSeconds", 300));
        var batchSize = Math.Clamp(_configuration.GetValue("Reconciliation:BatchSize", 500), 1, 1000);
        await _transactions.Indexes.CreateOneAsync(
            new CreateIndexModel<ReconciliationDocument>(
                Builders<ReconciliationDocument>.IndexKeys.Ascending(document => document.InvoiceId),
                new CreateIndexOptions { Unique = true, Name = "UQ_Transactions_InvoiceId" }),
            cancellationToken: stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var cutoff = DateTime.UtcNow.AddSeconds(-stabilityWindowSeconds);
                var discrepancies = await ReconcileSqlToMongoAsync(cutoff, batchSize, stoppingToken);
                discrepancies += await ReconcileMongoToSqlAsync(cutoff, batchSize, stoppingToken);
                _logger.LogInformation(
                    "Cross-store reconciliation completed at {Cutoff}; discrepancies: {Count}.",
                    cutoff,
                    discrepancies);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "Cross-store reconciliation failed.");
            }

            await Task.Delay(TimeSpan.FromSeconds(intervalSeconds), stoppingToken);
        }
    }

    private async Task<int> ReconcileSqlToMongoAsync(
        DateTime cutoff,
        int batchSize,
        CancellationToken cancellationToken)
    {
        var discrepancies = 0;
        Guid? lastInvoiceId = null;

        while (true)
        {
            var ledgerRows = await ReadSqlBatchAsync(lastInvoiceId, cutoff, batchSize, cancellationToken);
            if (ledgerRows.Count == 0)
            {
                return discrepancies;
            }

            lastInvoiceId = ledgerRows[^1].InvoiceId;
            var mongoFilter = Builders<ReconciliationDocument>.Filter.And(
                Builders<ReconciliationDocument>.Filter.Lt(document => document.ProcessedAt, cutoff),
                Builders<ReconciliationDocument>.Filter.In(document => document.InvoiceId, ledgerRows.Select(row => row.InvoiceId)));
            var mongoRows = await _transactions.Find(mongoFilter)
                .ToListAsync(cancellationToken);
            var mongoById = mongoRows.ToDictionary(row => row.InvoiceId);

            foreach (var ledgerRow in ledgerRows)
            {
                if (!mongoById.TryGetValue(ledgerRow.InvoiceId, out var mongoRow))
                {
                    discrepancies++;
                    _logger.LogWarning("SQL ledger invoice {InvoiceId} is missing from MongoDB.", ledgerRow.InvoiceId);
                }
                else if (ledgerRow.GrossAmount != mongoRow.GrossValue ||
                         ledgerRow.IbsAmount != mongoRow.IbsDeduction ||
                         ledgerRow.CbsAmount != mongoRow.CbsDeduction ||
                         ledgerRow.NetAmount != mongoRow.NetMerchantValue)
                {
                    discrepancies++;
                    _logger.LogWarning("Stored amounts differ for invoice {InvoiceId}.", ledgerRow.InvoiceId);
                }
            }

            if (ledgerRows.Count < batchSize)
            {
                return discrepancies;
            }
        }
    }

    private async Task<int> ReconcileMongoToSqlAsync(
        DateTime cutoff,
        int batchSize,
        CancellationToken cancellationToken)
    {
        var discrepancies = 0;
        Guid? lastInvoiceId = null;

        while (true)
        {
            var filter = Builders<ReconciliationDocument>.Filter.Lt(document => document.ProcessedAt, cutoff);
            if (lastInvoiceId.HasValue)
            {
                filter &= Builders<ReconciliationDocument>.Filter.Gt(document => document.InvoiceId, lastInvoiceId.Value);
            }

            var mongoRows = await _transactions.Find(filter)
                .SortBy(document => document.InvoiceId)
                .Limit(batchSize)
                .ToListAsync(cancellationToken);
            if (mongoRows.Count == 0)
            {
                return discrepancies;
            }

            lastInvoiceId = mongoRows[^1].InvoiceId;
            var sqlRows = await ReadSqlRowsByIdsAsync(mongoRows.Select(row => row.InvoiceId).ToArray(), cancellationToken);
            var sqlById = sqlRows.ToDictionary(row => row.InvoiceId);

            foreach (var mongoRow in mongoRows)
            {
                if (!sqlById.TryGetValue(mongoRow.InvoiceId, out var ledgerRow))
                {
                    discrepancies++;
                    _logger.LogWarning("MongoDB invoice {InvoiceId} is missing from SQL ledger.", mongoRow.InvoiceId);
                }
                else if (ledgerRow.GrossAmount != mongoRow.GrossValue ||
                         ledgerRow.IbsAmount != mongoRow.IbsDeduction ||
                         ledgerRow.CbsAmount != mongoRow.CbsDeduction ||
                         ledgerRow.NetAmount != mongoRow.NetMerchantValue)
                {
                    discrepancies++;
                    _logger.LogWarning("Stored amounts differ for invoice {InvoiceId}.", mongoRow.InvoiceId);
                }
            }

            if (mongoRows.Count < batchSize)
            {
                return discrepancies;
            }
        }
    }

    private async Task<List<LedgerRow>> ReadSqlBatchAsync(
        Guid? lastInvoiceId,
        DateTime cutoff,
        int batchSize,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT TOP (@BatchSize) InvoiceId, GrossAmount, IbsAmount, CbsAmount, NetAmount
            FROM dbo.CgibsTaxLedger
            WHERE ProcessedAt < @Cutoff AND (@LastInvoiceId IS NULL OR InvoiceId > @LastInvoiceId)
            ORDER BY InvoiceId;
            """;
        command.Parameters.Add("@BatchSize", SqlDbType.Int).Value = batchSize;
        command.Parameters.Add("@Cutoff", SqlDbType.DateTime2).Value = cutoff;
        command.Parameters.Add("@LastInvoiceId", SqlDbType.UniqueIdentifier).Value = (object?)lastInvoiceId ?? DBNull.Value;

        var rows = new List<LedgerRow>(batchSize);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new LedgerRow(
                reader.GetGuid(0),
                reader.GetDecimal(1),
                reader.GetDecimal(2),
                reader.GetDecimal(3),
                reader.GetDecimal(4)));
        }

        return rows;
    }

    private async Task<List<LedgerRow>> ReadSqlRowsByIdsAsync(Guid[] invoiceIds, CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        var parameterNames = new string[invoiceIds.Length];
        for (var index = 0; index < invoiceIds.Length; index++)
        {
            parameterNames[index] = $"@InvoiceId{index}";
            command.Parameters.Add(parameterNames[index], SqlDbType.UniqueIdentifier).Value = invoiceIds[index];
        }

        command.CommandText = $"""
            SELECT InvoiceId, GrossAmount, IbsAmount, CbsAmount, NetAmount
            FROM dbo.CgibsTaxLedger
            WHERE InvoiceId IN ({string.Join(", ", parameterNames)});
            """;

        var rows = new List<LedgerRow>(invoiceIds.Length);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new LedgerRow(
                reader.GetGuid(0),
                reader.GetDecimal(1),
                reader.GetDecimal(2),
                reader.GetDecimal(3),
                reader.GetDecimal(4)));
        }

        return rows;
    }

    private sealed record LedgerRow(Guid InvoiceId, decimal GrossAmount, decimal IbsAmount, decimal CbsAmount, decimal NetAmount);
}

public sealed class ReconciliationDocument
{
    [BsonId]
    public string Id { get; set; } = string.Empty;
    [BsonElement("invoiceId")]
    [BsonRepresentation(BsonType.String)]
    public Guid InvoiceId { get; set; }
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