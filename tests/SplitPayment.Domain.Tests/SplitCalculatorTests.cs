using SplitPayment.Domain.Calculations;
using SplitPayment.Domain.Events;

namespace SplitPayment.Domain.Tests;

public sealed class SplitCalculatorTests
{
    [Fact]
    public void Calculate_UsesRatesWithoutRoundingOutsideSimplifiedProcedure()
    {
        var result = SplitCalculator.Calculate(CreateInvoice(100m, 17.7m, 8.8m));

        Assert.Equal(17.7m, result.IbsAmount);
        Assert.Equal(8.8m, result.CbsAmount);
        Assert.Equal(73.5m, result.NetAmount);
    }

    [Fact]
    public void Calculate_PreservesAllSupportedPrecisionAndExactConservation()
    {
        const decimal grossAmount = 1.0001m;
        var result = SplitCalculator.Calculate(CreateInvoice(grossAmount, 1.000001m, 0m));

        Assert.Equal(0.010001010001m, result.IbsAmount);
        Assert.Equal(0m, result.CbsAmount);
        Assert.Equal(grossAmount, result.IbsAmount + result.CbsAmount + result.NetAmount);
    }

    [Fact]
    public void Calculate_TruncatesEachTaxTowardZeroInSimplifiedProcedure()
    {
        var result = SplitCalculator.Calculate(CreateInvoice(10.01m, 17.7m, 8.8m, true));

        Assert.Equal(1.77m, result.IbsAmount);
        Assert.Equal(0.88m, result.CbsAmount);
        Assert.Equal(7.36m, result.NetAmount);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Calculate_RejectsNonPositiveGrossAmount(decimal grossAmount)
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => SplitCalculator.Calculate(CreateInvoice(grossAmount, 0m, 0m)));
    }

    [Theory]
    [InlineData(-0.01)]
    [InlineData(100.01)]
    public void Calculate_RejectsRatesOutsideZeroToOneHundred(decimal rate)
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => SplitCalculator.Calculate(CreateInvoice(100m, rate, 0m)));
    }

    [Fact]
    public void Calculate_RejectsCombinedRatesAboveOneHundredPercent()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => SplitCalculator.Calculate(CreateInvoice(100m, 60m, 40.01m)));
    }

    private static InvoiceCreatedEvent CreateInvoice(
        decimal grossAmount,
        decimal ibsRate,
        decimal cbsRate,
        bool isSimplifiedProcedure = false) =>
        new(
            Guid.NewGuid(),
            "merchant",
            "customer",
            grossAmount,
            ibsRate,
            cbsRate,
            isSimplifiedProcedure,
            DateTime.UtcNow);
}