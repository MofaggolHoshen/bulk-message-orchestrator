# bulk-message-orchestrator

Scalable bulk message publishing system built with ASP.NET Core, Hangfire, MassTransit, and Azure Service Bus.

## Features

- Configurable job creation endpoint: `POST /api/message-jobs`
- Hangfire enqueue/scheduling with recurring cron registrations from appsettings
- MassTransit publish abstraction with Azure Service Bus configuration and retry policy
- High-throughput bulk publishing via `Chunk(batchSize)` + `Parallel.ForEachAsync`
- Realtime progress tracking via SignalR hub (`/hubs/progress`) and progress API
- Failure tracking (`FailedMessages`) and execution audit logs (`JobExecutionLogs`)
- Health checks (`/health`) and Hangfire dashboard (`/hangfire`)

## Run

```bash
dotnet restore
dotnet run --project /home/runner/work/bulk-message-orchestrator/bulk-message-orchestrator/src/BulkMessage.Orchestrator.Api/BulkMessage.Orchestrator.Api.csproj
```

## Configuration

Use `src/BulkMessage.Orchestrator.Api/appsettings.json`:

- `ConnectionStrings:SqlServer`
- `ConnectionStrings:Hangfire`
- `ConnectionStrings:AzureServiceBus`
- `BulkPublishing`
- `RecurringSchedules`
