namespace SplitPayment.Domain.Validation;

public static class TaxDocumentValidator
{
    public static bool IsValid(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        if (value.Any(character => !char.IsAsciiLetterOrDigit(character) &&
            character is not ('.' or '-' or '/' or ' ')))
        {
            return false;
        }

        var normalized = Normalize(value);
        return normalized.Length switch
        {
            11 => normalized.All(char.IsAsciiDigit) &&
                IsValidWithCheckDigits(normalized, [10, 9, 8, 7, 6, 5, 4, 3, 2], [11, 10, 9, 8, 7, 6, 5, 4, 3, 2]),
            14 => IsValidCnpj(normalized),
            _ => false
        };
    }

    public static string Normalize(string value) => new(value.Where(char.IsAsciiLetterOrDigit).Select(char.ToUpperInvariant).ToArray());

    private static bool IsValidCnpj(string value)
    {
        if (!value[..8].All(char.IsAsciiLetterOrDigit) ||
            !value[8..].All(char.IsAsciiDigit))
        {
            return false;
        }

        return IsValidWithCheckDigits(value, [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2], [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]);
    }

    private static bool IsValidWithCheckDigits(string digits, int[] firstWeights, int[] secondWeights)
    {
        if (digits.Distinct().Count() == 1)
        {
            return false;
        }

        var firstDigit = CalculateCheckDigit(digits, firstWeights);
        var secondDigit = CalculateCheckDigit(digits[..^1], secondWeights);
        return digits[^2] - '0' == firstDigit && digits[^1] - '0' == secondDigit;
    }

    private static int CalculateCheckDigit(string digits, int[] weights)
    {
        var sum = 0;
        for (var index = 0; index < weights.Length; index++)
        {
            sum += (digits[index] - '0') * weights[index];
        }

        var remainder = sum % 11;
        return remainder < 2 ? 0 : 11 - remainder;
    }
}