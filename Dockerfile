FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY ["src/BulkMessage.Orchestrator.Api/BulkMessage.Orchestrator.Api.csproj", "src/BulkMessage.Orchestrator.Api/"]
RUN dotnet restore "src/BulkMessage.Orchestrator.Api/BulkMessage.Orchestrator.Api.csproj"
COPY . .
WORKDIR "/src/src/BulkMessage.Orchestrator.Api"
RUN dotnet build "BulkMessage.Orchestrator.Api.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "BulkMessage.Orchestrator.Api.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "BulkMessage.Orchestrator.Api.dll"]
