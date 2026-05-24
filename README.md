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
- API key authentication and rate limiting (60 requests/minute on job creation)

## Setup & Installation

### Prerequisites

- .NET 10.0 SDK or later
- SQL Server (or in-memory database for development)
- Azure Service Bus connection string (or in-memory transport for development)

### Quick Start (Development)

1. **Clone the repository:**

   ```bash
   git clone https://github.com/MofaggolHoshen/bulk-message-orchestrator.git
   cd bulk-message-orchestrator
   ```

2. **Restore dependencies:**

   ```bash
   dotnet restore
   ```

3. **Run the API (with in-memory storage for testing):**

   ```bash
   dotnet run --project src/BulkMessage.Orchestrator.WithHangfire.Api/BulkMessage.Orchestrator.WithHangfire.Api.csproj
   ```

   The API will be available at `https://localhost:5001`

4. **Access endpoints:**
   - API Documentation: `https://localhost:5001/openapi/v1.json`
   - Health Check: `https://localhost:5001/health`
   - Hangfire Dashboard: `https://localhost:5001/dashboard`
   - SignalR Progress Hub: `wss://localhost:5001/hubs/progress`

### Production Configuration

Update `src/BulkMessage.Orchestrator.WithHangfire.Api/appsettings.json` with your settings:

```json
{
  "ApiKey": "your-secure-api-key",
  "ConnectionStrings": {
    "SqlServer": "Server=your-server;Database=bulk-orchestrator;User Id=sa;Password=YourPassword;",
    "Hangfire": "Server=your-server;Database=hangfire;User Id=sa;Password=YourPassword;",
    "AzureServiceBus": "Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your-key"
  },
  "BulkPublishing": {
    "DefaultBatchSize": 1000,
    "MaxParallelPublishes": 16,
    "RetryCount": 3
  },
  "RecurringSchedules": []
}
```

## API Documentation

### Authentication

All API endpoints (except `/health`) require an API key header:

```
Authorization: Bearer {your-api-key}
```

In development (when `ApiKey` is empty), authentication is bypassed.

### Endpoints

#### 1. Create a Bulk Publish Job

**POST** `/api/message-jobs`

Creates a new bulk message publishing job.

**Request Body:**

```json
{
  "messageCount": 100000,
  "batchSize": 1000,
  "maxParallelPublishes": 16,
  "payloadTemplate": "message-{0}",
  "scheduleAtUtc": null
}
```

**Parameters:**

- `messageCount` (integer, required): Total number of messages to publish
- `batchSize` (integer, optional): Messages per batch (default: 1000)
- `maxParallelPublishes` (integer, optional): Concurrent batch publishers (default: 16)
- `payloadTemplate` (string, required): Template for message payload (use `{0}` for counter)
- `scheduleAtUtc` (datetime, optional): Schedule job for later execution (UTC). Omit or null to execute immediately

**Response (202 Accepted):**

```json
{
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "hangfireJobId": "1234567890",
  "scheduledAtUtc": null
}
```

**Rate Limit:** 60 requests per minute

**Example:**

```bash
curl -X POST https://localhost:5001/api/message-jobs \
  -H "Authorization: Bearer your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "messageCount": 50000,
    "payloadTemplate": "user-event-{0}",
    "scheduleAtUtc": null
  }'
```

---

#### 2. Get Job Progress

**GET** `/api/message-jobs/{jobId}/progress`

Retrieve real-time progress for a job.

**Response (200 OK):**

```json
{
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "totalMessages": 100000,
  "publishedMessages": 45000,
  "failedMessages": 150,
  "completed": false
}
```

**Example:**

```bash
curl https://localhost:5001/api/message-jobs/550e8400-e29b-41d4-a716-446655440000/progress \
  -H "Authorization: Bearer your-api-key"
```

---

#### 3. Cancel a Job

**POST** `/api/message-jobs/{jobId}/cancel`

Cancel a queued, scheduled, or running job.

**Response (202 Accepted):**

```json
{}
```

**Error (409 Conflict):** Job cannot be cancelled (already completed/failed)

**Example:**

```bash
curl -X POST https://localhost:5001/api/message-jobs/550e8400-e29b-41d4-a716-446655440000/cancel \
  -H "Authorization: Bearer your-api-key"
```

---

#### 4. Retry Failed Messages

**POST** `/api/message-jobs/{jobId}/retry`

Create a new job to retry all failed messages from a previous job.

**Response (202 Accepted):**

```json
{
  "jobId": "660e8400-e29b-41d4-a716-446655440001",
  "hangfireJobId": "1234567891",
  "scheduledAtUtc": null
}
```

**Error (404 Not Found):** Job not found
**Error (409 Conflict):** No failed messages to retry

**Example:**

```bash
curl -X POST https://localhost:5001/api/message-jobs/550e8400-e29b-41d4-a716-446655440000/retry \
  -H "Authorization: Bearer your-api-key"
```

---

#### 5. Health Check

**GET** `/health`

Check API and database health (no authentication required).

**Response (200 OK):**

```json
{
  "status": "Healthy"
}
```

---

### Real-time Progress Tracking (SignalR)

Connect to the SignalR hub for real-time job progress updates:

**Hub URL:** `wss://localhost:5001/hubs/progress`

**Events:**

- `OnProgressUpdate` — Fired when progress changes

**Example (JavaScript):**

```javascript
const connection = new signalR.HubConnectionBuilder()
  .withUrl("wss://localhost:5001/hubs/progress")
  .withAutomaticReconnect()
  .build();

connection.on("OnProgressUpdate", (progress) => {
  console.log(
    `Job ${progress.jobId}: ${progress.publishedMessages}/${progress.totalMessages} published`,
  );
});

connection.start().catch((err) => console.error(err));
```

---

### Hangfire Dashboard

Access the Hangfire job dashboard at: `https://localhost:5001/dashboard`

The dashboard provides:

- Active job monitoring
- Failed job inspection
- Recurring job configuration
- Job history and logs

---

## Configuration Reference

### appsettings.json

| Key                                   | Description                                      | Example                                         |
| ------------------------------------- | ------------------------------------------------ | ----------------------------------------------- |
| `ApiKey`                              | API authentication key (empty = no auth in dev)  | `"secure-key-123"`                              |
| `ConnectionStrings:SqlServer`         | SQL Server connection (empty = in-memory)        | `"Server=localhost;Database=bulk-orchestrator"` |
| `ConnectionStrings:Hangfire`          | Hangfire storage connection (empty = memory)     | `"Server=localhost;Database=hangfire"`          |
| `ConnectionStrings:AzureServiceBus`   | Azure Service Bus connection (empty = in-memory) | `"Endpoint=sb://...;SharedAccessKey=..."`       |
| `BulkPublishing:DefaultBatchSize`     | Default messages per batch                       | `1000`                                          |
| `BulkPublishing:MaxParallelPublishes` | Default concurrent batch publishers              | `16`                                            |
| `BulkPublishing:RetryCount`           | Message publication retry attempts               | `3`                                             |
| `RecurringSchedules`                  | Array of scheduled jobs (cron-based)             | See example below                               |

### Recurring Schedules Example

```json
"RecurringSchedules": [
  {
    "Id": "hourly-health-probe",
    "Cron": "0 * * * *",
    "Enabled": true,
    "MessageCount": 100,
    "PayloadTemplate": "health-probe-{0}"
  }
]
```

**Cron Format:** `minute hour day month day-of-week`

- `0 * * * *` = Every hour
- `0 2 L * *` = Every month-end at 2 AM UTC

---

## Architecture

### Components

- **MessageJobsController** — RESTful API for job management
- **BulkPublishingEngine** — Executes bulk publishing with parallel batching
- **ProgressHub (SignalR)** — Real-time progress broadcasting
- **OrchestratorDbContext** — Entity Framework data access layer
- **Hangfire** — Background job scheduling and execution
- **MassTransit** — Message broker abstraction (Azure Service Bus/in-memory)

### Job Lifecycle

1. **Created** → Job submitted via API
2. **Queued/Scheduled** → Waiting in Hangfire queue or scheduled for future
3. **Running** → BulkPublishingEngine executing batches
4. **Completed** → All messages published or failed
5. **Failed Messages Tracked** → Stored in `FailedMessages` table for retry

---

## Troubleshooting

### Jobs Stuck in "Running" State

Check Hangfire dashboard for long-running jobs. If crashed, restart the application to clean up state.

### High Memory Usage

Reduce `MaxParallelPublishes` in configuration to lower concurrent batch processing.

### Message Publish Failures

Check Azure Service Bus connection and ensure the service principal has `Send` permissions. Failed messages are logged and can be retried via `/retry` endpoint.

---

## License

MIT License - See LICENSE file for details
