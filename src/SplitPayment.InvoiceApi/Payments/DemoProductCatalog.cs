namespace SplitPayment.InvoiceApi.Payments;

public sealed record DemoProduct(string Id, string Name, string Description, decimal UnitPrice);

public static class DemoProductCatalog
{
    private static readonly IReadOnlyList<DemoProduct> Products =
    [
        new("demo-item-001", "Produto demonstrativo", "Item fictício para testar o checkout em sandbox.", 49.90m)
    ];

    public static IReadOnlyList<DemoProduct> GetAll() => Products;

    public static bool TryGet(string productId, out DemoProduct product)
    {
        product = Products.FirstOrDefault(item =>
            string.Equals(item.Id, productId, StringComparison.OrdinalIgnoreCase))!;
        return product is not null;
    }
}
