using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace SplitPayment.InvoiceApi.Payments;

public static class MercadoPagoWebhookSignatureValidator
{
    private static readonly TimeSpan AllowedClockSkew = TimeSpan.FromMinutes(5);

    public static bool IsValid(
        string? signatureHeader,
        string? requestId,
        string? dataId,
        string secret,
        DateTimeOffset now)
    {
        if (string.IsNullOrWhiteSpace(signatureHeader) ||
            string.IsNullOrWhiteSpace(requestId) ||
            string.IsNullOrWhiteSpace(dataId) ||
            string.IsNullOrWhiteSpace(secret) ||
            requestId.Length > 128 ||
            dataId.Length > 64)
        {
            return false;
        }

        string? timestamp = null;
        string? suppliedHash = null;
        foreach (var part in signatureHeader.Split(','))
        {
            var pair = part.Trim().Split('=', 2);
            if (pair.Length != 2)
            {
                return false;
            }

            if (pair[0] == "ts")
            {
                if (timestamp is not null)
                {
                    return false;
                }

                timestamp = pair[1];
            }
            else if (pair[0] == "v1")
            {
                if (suppliedHash is not null)
                {
                    return false;
                }

                suppliedHash = pair[1];
            }
        }

        if (timestamp is null ||
            suppliedHash is null ||
            !long.TryParse(timestamp, NumberStyles.None, CultureInfo.InvariantCulture, out var timestampMilliseconds) ||
            !TryGetTimestamp(timestampMilliseconds, out var sentAt) ||
            (now - sentAt).Duration() > AllowedClockSkew)
        {
            return false;
        }

        var manifest = $"id:{dataId.ToLowerInvariant()};request-id:{requestId};ts:{timestamp};";
        var expectedHash = HMACSHA256.HashData(Encoding.UTF8.GetBytes(secret), Encoding.UTF8.GetBytes(manifest));
        if (suppliedHash.Length != expectedHash.Length * 2)
        {
            return false;
        }

        byte[] actualHash;
        try
        {
            actualHash = Convert.FromHexString(suppliedHash);
        }
        catch (FormatException)
        {
            return false;
        }

        return CryptographicOperations.FixedTimeEquals(expectedHash, actualHash);
    }

    private static bool TryGetTimestamp(long milliseconds, out DateTimeOffset timestamp)
    {
        try
        {
            timestamp = DateTimeOffset.FromUnixTimeMilliseconds(milliseconds);
            return true;
        }
        catch (ArgumentOutOfRangeException)
        {
            timestamp = default;
            return false;
        }
    }
}
