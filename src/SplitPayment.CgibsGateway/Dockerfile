FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY src/SplitPayment.Domain/SplitPayment.Domain.csproj src/SplitPayment.Domain/
COPY src/SplitPayment.CgibsGateway/SplitPayment.CgibsGateway.csproj src/SplitPayment.CgibsGateway/
RUN dotnet restore src/SplitPayment.CgibsGateway/SplitPayment.CgibsGateway.csproj
COPY src/ src/
RUN dotnet publish src/SplitPayment.CgibsGateway/SplitPayment.CgibsGateway.csproj \
    --configuration Release --no-restore --output /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
USER $APP_UID
ENTRYPOINT ["dotnet", "SplitPayment.CgibsGateway.dll"]