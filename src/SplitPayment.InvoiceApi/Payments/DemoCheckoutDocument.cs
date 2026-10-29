using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace SplitPayment.InvoiceApi.Payments;

public sealed class DemoCheckoutDocument
{
    [BsonId]
    public string IdempotencyKey { get; set; } = string.Empty;
    [BsonElement("checkoutId")]
    public string CheckoutId { get; set; } = string.Empty;
    [BsonElement("requestHash")]
    public string RequestHash { get; set; } = string.Empty;
    [BsonElement("productId")]
    public string ProductId { get; set; } = string.Empty;
    [BsonElement("productName")]
    public string ProductName { get; set; } = string.Empty;
    [BsonElement("unitPrice")]
    [BsonRepresentation(BsonType.Decimal128)]
    public decimal UnitPrice { get; set; }
    [BsonElement("quantity")]
    public int Quantity { get; set; }
    [BsonElement("totalAmount")]
    [BsonRepresentation(BsonType.Decimal128)]
    public decimal TotalAmount { get; set; }
    [BsonElement("status")]
    public string Status { get; set; } = "Creating";
    [BsonElement("providerPreferenceId")]
    public string? ProviderPreferenceId { get; set; }
    [BsonElement("sandboxCheckoutUrl")]
    public string? SandboxCheckoutUrl { get; set; }
    [BsonElement("mockCheckoutUrl")]
    public string? MockCheckoutUrl { get; set; }
    [BsonElement("isMockPayment")]
    public bool IsMockPayment { get; set; }
    [BsonElement("testProfileId")]
    public string? TestProfileId { get; set; }
    [BsonElement("testProfileName")]
    public string? TestProfileName { get; set; }
    [BsonElement("providerPaymentId")]
    public string? ProviderPaymentId { get; set; }
    [BsonElement("createdAt")]
    public DateTime CreatedAt { get; set; }
    [BsonElement("updatedAt")]
    public DateTime UpdatedAt { get; set; }
}
