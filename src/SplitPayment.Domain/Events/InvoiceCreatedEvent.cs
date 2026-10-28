using System.Text.Json.Serialization;

namespace SplitPayment.Domain.Events;

public sealed record InvoiceCreatedEvent(
    [property: JsonPropertyName("invoiceId")]
    Guid InvoiceId,
    [property: JsonPropertyName("merchantTaxId")]
    string MerchantTaxId,
    [property: JsonPropertyName("customerTaxId")]
    string CustomerTaxId,
    [property: JsonPropertyName("grossAmount")]
    decimal GrossAmount,
    [property: JsonPropertyName("ibsRate")]
    decimal IbsRate,
    [property: JsonPropertyName("cbsRate")]
    decimal CbsRate,
    [property: JsonPropertyName("isSimplifiedProcedure")]
    bool IsSimplifiedProcedure,
    [property: JsonPropertyName("createdAt")]
    DateTime CreatedAt)
{
    public const int CurrentSchemaVersion = 1;

    [JsonPropertyName("schemaVersion")]
    public int SchemaVersion { get; init; } = CurrentSchemaVersion;
}