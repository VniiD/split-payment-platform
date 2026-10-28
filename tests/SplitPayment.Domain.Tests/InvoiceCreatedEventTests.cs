using System.Text.Json;
using SplitPayment.Domain.Events;

namespace SplitPayment.Domain.Tests;

public sealed class InvoiceCreatedEventTests
{
    [Fact]
    public void Serialization_EmitsVersionedCamelCaseContract()
    {
        var invoice = new InvoiceCreatedEvent(
            Guid.NewGuid(),
            "11222333000181",
            "52998224725",
            100m,
            17.7m,
            8.8m,
            false,
            DateTime.UtcNow);

        var json = JsonSerializer.Serialize(invoice, new JsonSerializerOptions(JsonSerializerDefaults.Web));
        var deserialized = JsonSerializer.Deserialize<InvoiceCreatedEvent>(
            json,
            new JsonSerializerOptions(JsonSerializerDefaults.Web));

        Assert.Contains("\"schemaVersion\":1", json);
        Assert.NotNull(deserialized);
        Assert.Equal(InvoiceCreatedEvent.CurrentSchemaVersion, deserialized.SchemaVersion);
        Assert.Equal(invoice.InvoiceId, deserialized.InvoiceId);
    }
}