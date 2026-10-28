FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY src/SplitPayment.Domain/SplitPayment.Domain.csproj src/SplitPayment.Domain/
COPY src/SplitPayment.InvoiceApi/SplitPayment.InvoiceApi.csproj src/SplitPayment.InvoiceApi/
RUN dotnet restore src/SplitPayment.InvoiceApi/SplitPayment.InvoiceApi.csproj
COPY src/ src/
RUN dotnet publish src/SplitPayment.InvoiceApi/SplitPayment.InvoiceApi.csproj \
    --configuration Release --no-restore --output /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app
RUN apt-get update \
    && apt-get install --yes --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/publish .
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080
USER $APP_UID
ENTRYPOINT ["dotnet", "SplitPayment.InvoiceApi.dll"]