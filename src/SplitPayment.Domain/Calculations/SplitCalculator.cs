using SplitPayment.Domain.Events;

namespace SplitPayment.Domain.Calculations;

public static class SplitCalculator
{
    public static SplitCalculation Calculate(InvoiceCreatedEvent invoice)
    {
        ArgumentNullException.ThrowIfNull(invoice);

        if (invoice.GrossAmount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(invoice), "Gross amount must be greater than zero.");
        }

        if (GetScale(invoice.GrossAmount) > 4 || invoice.GrossAmount > 99_999_999_999_999.9999m)
        {
            throw new ArgumentOutOfRangeException(nameof(invoice), "Gross amount supports at most 14 integer and 4 fractional digits.");
        }

        ValidateRate(invoice.IbsRate, nameof(invoice.IbsRate));
        ValidateRate(invoice.CbsRate, nameof(invoice.CbsRate));

        if (invoice.IbsRate + invoice.CbsRate > 100m)
        {
            throw new ArgumentOutOfRangeException(nameof(invoice), "Combined rates cannot exceed one hundred percent.");
        }

        var ibsAmount = invoice.GrossAmount * invoice.IbsRate / 100m;
        var cbsAmount = invoice.GrossAmount * invoice.CbsRate / 100m;

        if (invoice.IsSimplifiedProcedure)
        {
            ibsAmount = decimal.Round(ibsAmount, 2, MidpointRounding.ToZero);
            cbsAmount = decimal.Round(cbsAmount, 2, MidpointRounding.ToZero);
        }

        return new SplitCalculation(
            ibsAmount,
            cbsAmount,
            invoice.GrossAmount - ibsAmount - cbsAmount);
    }

    private static void ValidateRate(decimal rate, string parameterName)
    {
        if (rate is < 0 or > 100 || GetScale(rate) > 6)
        {
            throw new ArgumentOutOfRangeException(parameterName, "Rate must be between zero and one hundred with at most six fractional digits.");
        }
    }

    private static int GetScale(decimal value) => (decimal.GetBits(value)[3] >> 16) & 0x7F;
}