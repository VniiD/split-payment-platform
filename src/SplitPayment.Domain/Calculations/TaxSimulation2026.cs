namespace SplitPayment.Domain.Calculations;

public sealed record TaxSimulation2026(
    string RuleSetId,
    string Status,
    string Scenario,
    DateOnly ReferenceDate,
    decimal TaxBase,
    bool TaxBaseIsSynthetic,
    decimal IbsRate,
    decimal IbsInformationalAmount,
    string IbsCompetentAuthorities,
    decimal CbsRate,
    decimal CbsInformationalAmount,
    string CbsCompetentAuthority,
    decimal TotalInformationalAmount,
    bool FiscalClassificationValidated,
    bool PaymentOrTaxCollectionExecuted,
    string Assumptions,
    IReadOnlyList<string> OfficialSources);

public static class TaxSimulation2026Calculator
{
    public const string RuleSetId = "BR-RTC-2026-TEST-1";

    private static readonly DateOnly PeriodStart = new(2026, 1, 1);
    private static readonly DateOnly PeriodEnd = new(2026, 12, 31);

    public static TaxSimulation2026 Calculate(decimal grossAmount, DateOnly referenceDate)
    {
        if (referenceDate < PeriodStart || referenceDate > PeriodEnd)
        {
            throw new ArgumentOutOfRangeException(
                nameof(referenceDate),
                "This demonstrative rule set only applies to reference dates in 2026.");
        }

        if (grossAmount <= 0 ||
            GetScale(grossAmount) > 2 ||
            grossAmount > 99_999_999_999_999.99m)
        {
            throw new ArgumentOutOfRangeException(
                nameof(grossAmount),
                "The demonstrative tax base must be positive, have at most 14 integer digits, and use at most two decimal places.");
        }

        const decimal ibsRate = 0.1m;
        const decimal cbsRate = 0.9m;
        var ibsAmount = decimal.Round(grossAmount * ibsRate / 100m, 2, MidpointRounding.AwayFromZero);
        var cbsAmount = decimal.Round(grossAmount * cbsRate / 100m, 2, MidpointRounding.AwayFromZero);

        return new TaxSimulation2026(
            RuleSetId,
            "InformationalTestOnly",
            "SyntheticRegularRegimeGoodsSale",
            referenceDate,
            grossAmount,
            true,
            ibsRate,
            ibsAmount,
            "IBS: competência compartilhada entre Estados e Municípios; nenhum rateio por ente é calculado nesta simulação.",
            cbsRate,
            cbsAmount,
            "CBS: União (Receita Federal do Brasil); nenhum recolhimento é realizado nesta simulação.",
            ibsAmount + cbsAmount,
            false,
            false,
            "Cenário sintético de venda de mercadoria no regime regular. A base é fornecida pelo chamador e não foi derivada de uma operação, NCM, CST ou cClassTrib validados. O enquadramento, a base tributável, a obrigação acessória e a eventual dispensa de recolhimento não são determinados. O arredondamento em centavos é demonstrativo e não foi estabelecido como regra fiscal. Este resultado não calcula liquidação do vendedor nem executa pagamento, recolhimento ou emissão de documento fiscal.",
            [
                "https://cgibs.gov.br/novo-marco-da-reforma-tributaria-inicia-em-03-de-agosto-com-preenchimento-obrigatorio-dos-campos-relativos-ao-ibs-e-a-cbs",
                "https://www.gov.br/receitafederal/pt-br/assuntos/noticias/2026/janeiro/fazenda-receita-e-serpro-lancam-reforma-tributaria-do-consumo-em-cerimonia-em-brasilia",
                "https://www.in.gov.br/web/dou/-/ato-conjunto-rfb/cgibs-n-1-de-22-de-dezembro-de-2025-677624586"
            ]);
    }

    private static int GetScale(decimal value) => (decimal.GetBits(value)[3] >> 16) & 0x7F;
}
