using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using SplitPayment.CgibsGateway;

namespace SplitPayment.Domain.Tests;

public sealed class ReconciliationDocumentTests
{
    [Fact]
    public void Deserialize_AcceptsEngineTransactionDocument()
    {
        var invoiceId = Guid.NewGuid();
        var bson = new BsonDocument
        {
            ["_id"] = invoiceId.ToString("D"),
            ["invoiceId"] = invoiceId.ToString("D"),
            ["merchantTaxId"] = "11222333000181",
            ["grossValue"] = new Decimal128(1000m),
            ["ibsDeduction"] = new Decimal128(177m),
            ["cbsDeduction"] = new Decimal128(88m),
            ["netMerchantValue"] = new Decimal128(735m),
            ["processedAt"] = DateTime.UtcNow
        };

        var document = BsonSerializer.Deserialize<ReconciliationDocument>(bson);

        Assert.Equal(invoiceId, document.InvoiceId);
        Assert.Equal("11222333000181", document.MerchantTaxId);
        Assert.Equal(177m, document.IbsDeduction);
        Assert.Equal(88m, document.CbsDeduction);
        Assert.Equal(735m, document.NetMerchantValue);
    }
}