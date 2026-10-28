using MongoDB.Bson.Serialization.Attributes;

namespace SplitPayment.InvoiceApi;

public sealed class InvoiceOutboxDocument
{
    [BsonId]
    public string Id { get; set; } = string.Empty;
    public string RequestHash { get; set; } = string.Empty;
    public string InvoiceId { get; set; } = string.Empty;
    public string EventJson { get; set; } = string.Empty;
    public string Status { get; set; } = "Pending";
    public DateTime CreatedAt { get; set; }
    public DateTime? LeaseUntil { get; set; }
    public DateTime? PublishedAt { get; set; }
    public int Attempts { get; set; }
    public string? LastErrorCode { get; set; }
}