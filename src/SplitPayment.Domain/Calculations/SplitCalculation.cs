namespace SplitPayment.Domain.Calculations;

public sealed record SplitCalculation(
    decimal IbsAmount,
    decimal CbsAmount,
    decimal NetAmount);