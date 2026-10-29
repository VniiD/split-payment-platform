using SplitPayment.InvoiceApi.Payments;

namespace SplitPayment.Domain.Tests;

public sealed class DemoTestProfileCatalogTests
{
    [Fact]
    public void SeedProfiles_AreSyntheticAndUseReservedTestEmails()
    {
        var profiles = DemoTestProfileCatalog.GetSeedProfiles();

        Assert.Equal(2, profiles.Count);
        Assert.All(profiles, profile =>
        {
            Assert.Equal("Buyer", profile.Role);
            Assert.True(profile.IsSynthetic);
            Assert.EndsWith(".test", profile.Email, StringComparison.Ordinal);
        });
        Assert.Equal(profiles.Count, profiles.Select(profile => profile.Id).Distinct().Count());
    }
}
