using SplitPayment.Domain.Calculations;

namespace SplitPayment.Domain.Tests;

public sealed class TaxSimulation2026Tests
{
    [Fact]
    public void Calculate_UsesPublishedInformationalRatesWithoutReducingSellerSettlement()
    {
        var result = TaxSimulation2026Calculator.Calculate(1000m, new DateOnly(2026, 10, 28));

        Assert.Equal("BR-RTC-2026-TEST-1", result.RuleSetId);
        Assert.Equal("InformationalTestOnly", result.Status);
        Assert.Equal("SyntheticRegularRegimeGoodsSale", result.Scenario);
        Assert.True(result.TaxBaseIsSynthetic);
        Assert.Equal(0.1m, result.IbsRate);
        Assert.Equal(1m, result.IbsInformationalAmount);
        Assert.Equal(0.9m, result.CbsRate);
        Assert.Equal(9m, result.CbsInformationalAmount);
        Assert.Equal(10m, result.TotalInformationalAmount);
        Assert.False(result.FiscalClassificationValidated);
        Assert.False(result.PaymentOrTaxCollectionExecuted);
        Assert.Contains("eventual dispensa de recolhimento não são determinados", result.Assumptions, StringComparison.Ordinal);
    }

    [Fact]
    public void Calculate_RoundsInformationalAmountsToCents()
    {
        var result = TaxSimulation2026Calculator.Calculate(10.05m, new DateOnly(2026, 10, 28));

        Assert.Equal(0.01m, result.IbsInformationalAmount);
        Assert.Equal(0.09m, result.CbsInformationalAmount);
        Assert.True(result.TaxBaseIsSynthetic);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(1.001)]
    public void Calculate_RejectsInvalidTaxBases(decimal grossAmount)
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => TaxSimulation2026Calculator.Calculate(grossAmount, new DateOnly(2026, 10, 28)));
    }

    [Theory]
    [InlineData(2025, 12, 31)]
    [InlineData(2027, 1, 1)]
    public void Calculate_RejectsDatesOutsideRuleSetPeriod(int year, int month, int day)
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => TaxSimulation2026Calculator.Calculate(100m, new DateOnly(year, month, day)));
    }
}
