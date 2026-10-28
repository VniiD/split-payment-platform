using SplitPayment.Domain.Validation;

namespace SplitPayment.Domain.Tests;

public sealed class TaxDocumentValidatorTests
{
    [Theory]
    [InlineData("529.982.247-25")]
    [InlineData("11.222.333/0001-81")]
    [InlineData("12.ABC.345/0001-88")]
    [InlineData("12abc345000188")]
    public void IsValid_AcceptsDocumentsWithValidCheckDigits(string value)
    {
        Assert.True(TaxDocumentValidator.IsValid(value));
    }

    [Theory]
    [InlineData("123.456.789-00")]
    [InlineData("11.222.333/0001-80")]
    [InlineData("111.111.111-11")]
    [InlineData("CPF: 529.982.247-25")]
    [InlineData("５２９９８２２４７２５")]
    [InlineData("12.ABC.345/01DE-88")]
    public void IsValid_RejectsInvalidDocuments(string value)
    {
        Assert.False(TaxDocumentValidator.IsValid(value));
    }
}