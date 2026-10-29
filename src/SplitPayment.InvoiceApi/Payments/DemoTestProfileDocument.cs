using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace SplitPayment.InvoiceApi.Payments;

public sealed class DemoTestProfileDocument
{
    [BsonId]
    public string Id { get; set; } = string.Empty;

    [BsonElement("displayName")]
    public string DisplayName { get; set; } = string.Empty;

    [BsonElement("email")]
    public string Email { get; set; } = string.Empty;

    [BsonElement("role")]
    public string Role { get; set; } = "Buyer";

    [BsonElement("isSynthetic")]
    public bool IsSynthetic { get; set; } = true;
}

public static class DemoTestProfileCatalog
{
    private static readonly IReadOnlyList<DemoTestProfileDocument> Profiles =
    [
        new()
        {
            Id = "buyer-demo-001",
            DisplayName = "Comprador de demonstração",
            Email = "comprador@example.test"
        },
        new()
        {
            Id = "buyer-demo-002",
            DisplayName = "Compradora de demonstração",
            Email = "compradora@example.test"
        }
    ];

    public static async Task SeedAsync(
        IMongoCollection<DemoTestProfileDocument> profiles,
        CancellationToken cancellationToken)
    {
        foreach (var profile in Profiles)
        {
            await profiles.ReplaceOneAsync(
                item => item.Id == profile.Id,
                profile,
                new ReplaceOptions { IsUpsert = true },
                cancellationToken);
        }
    }

    public static IReadOnlyList<DemoTestProfileDocument> GetSeedProfiles() => Profiles;
}
