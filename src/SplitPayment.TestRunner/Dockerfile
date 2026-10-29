FROM mcr.microsoft.com/dotnet/sdk:10.0
WORKDIR /workspace
COPY . .
RUN dotnet restore SplitPayment.sln
ENTRYPOINT ["dotnet", "test", "SplitPayment.sln", "--configuration", "Release", "--no-restore"]
