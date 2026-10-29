FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY src/SplitPayment.Web/SplitPayment.Web.csproj src/SplitPayment.Web/
RUN dotnet restore src/SplitPayment.Web/SplitPayment.Web.csproj
COPY src/SplitPayment.Web/ src/SplitPayment.Web/
RUN dotnet publish src/SplitPayment.Web/SplitPayment.Web.csproj \
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
ENTRYPOINT ["dotnet", "SplitPayment.Web.dll"]
