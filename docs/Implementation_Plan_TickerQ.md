# Bulk Message Orchestrator — TickerQ Implementation Plan

## Overview

**Bulk Message Orchestrator with TickerQ** is an ASP.NET Core Web API that orchestrates large-scale message publishing (100,000+ messages) to Azure Service Bus via MassTransit. It uses **TickerQ** (a lightweight, distributed task queue built on top of Entity Framework Core) for durable background processing and scheduled jobs, Entity Framework Core for persistence, and SignalR for real-time progress streaming to connected clients.

The system is designed to:

- Accept a publish request via HTTP and return immediately with a `JobId`
- Enqueue or schedule a TickerQ background job to do the heavy lifting
- Chunk messages into batches and publish them in parallel via MassTransit
- Persist job state, failed messages, and execution logs to SQL Server
- Push live progress updates to subscribed clients through a SignalR hub
- Fall back gracefully to in-memory transports and databases for local development

### Key Difference from Hangfire

TickerQ is a **lightweight, EF Core-based task queue** that leverages the same database as the application, reducing infrastructure complexity. Unlike Hangfire (which requires separate storage), TickerQ stores jobs in the application's database and provides:

- Built-in entity-based job scheduling
- Database-driven job processing (no separate background service needed initially)
- Simplified retry logic integrated with EF Core transactions
- Easier testing with in-memory database support
- Lower operational overhead (no Hangfire server to manage separately)

---

## Phase Status Legend

| Symbol | Meaning                   |
| ------ | ------------------------- |
| ✅     | Completed                 |
| 🔄     | Partially implemented     |
| ⬜     | Planned — not yet started |
| 🔮     | Future enhancement        |

---

## Implementation Phases

| Phase                                      | Status | Objective                            | Summary                                                                                                                                                                                                                        |
| ------------------------------------------ | ------ | ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Phase 1 — Project Foundation               | ⬜     | Initialize core infrastructure       | ASP.NET Core Web API bootstrapped with dual-mode infrastructure: SQL Server or EF in-memory DB, TickerQ with SQL or memory job store, MassTransit with Azure Service Bus or in-memory transport.                               |
| Phase 2 — API Design                       | ⬜     | Create bulk publishing endpoints     | `POST /api/message-jobs`, `GET /{id}/progress`, `POST /{id}/cancel`, and `POST /{id}/retry` endpoints. Returns `202 Accepted` with `JobId` on creation.                                                                        |
| Phase 3 — Database Schema                  | ⬜     | Design persistence layer             | EF Core entities for `MessagePublishJob`, `FailedMessage`, `JobExecutionLog`, and `TickerQJob`. DbContext configured with constraints, indexes, and SQL Server/in-memory support.                                              |
| Phase 4 — TickerQ Integration              | ⬜     | Wire up TickerQ as job orchestrator  | TickerQ configured in `Program.cs` with job definitions for `BulkPublishJob` and `RetryFailedJob`. Jobs persisted to DB. Automatic retry and dequeue mechanisms configured.                                                    |
| Phase 5 — Scheduling System                | ⬜     | Implement configurable scheduling    | `RecurringScheduleOptions` extended with `MessageCount`, `BatchSize`, `MaxParallelPublishes`, `PayloadTemplate`. `ScheduledBulkPublishingJob.ExecuteAsync(schedule)` creates a job record and enqueues processing via TickerQ. |
| Phase 6 — MassTransit Integration          | ⬜     | Configure messaging infrastructure   | MassTransit registered with retry policy (3 attempts, 2-second interval), in-memory outbox, and Azure Service Bus or in-memory transport. `BulkMessagePublished` contract defined.                                             |
| Phase 7 — Background Processing Engine     | ⬜     | Build scalable publisher             | `BulkPublishingEngine` chunks messages by `BatchSize`, publishes in parallel via `Parallel.ForEachAsync` with configurable `MaxDegreeOfParallelism`, and persists progress after each batch.                                   |
| Phase 8 — Progress Tracking                | ⬜     | Enable real-time progress visibility | `InMemoryBulkPublishProgressStore` uses `ConcurrentDictionary` to track total, published, failed, and completion state per job. Falls back to DB for jobs not in memory.                                                       |
| Phase 9 — SignalR Integration              | ⬜     | Add real-time client updates         | `ProgressHub` allows clients to subscribe to a job group. The engine pushes `progress-updated` events after every batch and on completion.                                                                                     |
| Phase 10 — Failure Handling & Retry        | ⬜     | Improve reliability                  | Failed messages persisted to `FailedMessages`. `POST /{id}/retry` creates a retry job via TickerQ. `RetryFailedAsync` re-publishes failures, removes succeeded entries. TickerQ retry logic active.                            |
| Phase 11 — Monitoring & Observability      | ⬜     | Add operational visibility           | TickerQ Dashboard or custom admin UI to view queued jobs and job history. Health check at `/health`. Structured logging throughout. Open when `ApiKey` is empty (dev mode).                                                    |
| Phase 12 — Security & Production Hardening | ⬜     | Prepare for production deployment    | `ApiKeyAuthenticationHandler` with `[Authorize]` on all endpoints. Fixed-window rate limiter (60 req/min) on create. `ApiKey` config key (empty = open dev mode). Admin dashboard protected by same key.                       |
| Phase 13 — Deployment & Scaling            | ⬜     | Enable cloud scalability             | Multi-stage `Dockerfile` targeting .NET 10. `.dockerignore` included. `.github/workflows/ci.yml` with restore → build → test → Docker build pipeline.                                                                          |
| Phase 14 — Future Enhancements             | 🔮     | Extend platform capabilities         | Distributed TickerQ worker orchestration, multi-tenant scheduling, transactional outbox pattern, Redis-backed progress store, analytics dashboard, and message replay UI.                                                      |

---

## Detailed Phase Descriptions

### Phase 1 — Project Foundation ⬜

**Goal:** Bootstrap the project with all required infrastructure wired to dual-mode transports.

**What needs to be done:**

- ASP.NET Core Web API created with `Program.cs` as the composition root
- **Database**: If `ConnectionStrings:SqlServer` is present, EF Core uses `UseSqlServer`; otherwise falls back to `UseInMemoryDatabase("orchestrator-db")` for local development
- **TickerQ**: Installed via NuGet: `install-package TickerQ`. Register in DI container with `services.AddTickerQ()`. If `ConnectionStrings:SqlServer` is present, TickerQ uses SQL Server job store; otherwise uses in-memory job store
- **MassTransit**: If `ConnectionStrings:AzureServiceBus` is present, uses `UsingAzureServiceBus`; otherwise uses `UsingInMemory` — both configured with the same retry and outbox pipeline
- `BulkPublishingOptions` bound from `appsettings.json` under `"BulkPublishing"` (configured default batch size: 1000, max parallelism: 16, retry count: 3)
- Health checks registered for `OrchestratorDbContext` and TickerQ job queue

**Key files:** `Program.cs`, `appsettings.json`, `BulkPublishingOptions.cs`

---

### Phase 2 — API Design ⬜

**Goal:** Expose HTTP endpoints that decouple the caller from background processing.

**What needs to be done:**

- `POST /api/message-jobs` — Accepts `BulkPublishRequest` (message count, optional batch size, parallelism, payload template, optional `ScheduleAtUtc`). Creates a `MessagePublishJob` record, initializes the progress store, enqueues or schedules a TickerQ job, and returns `202 Accepted` with `{ jobId, tickerQJobId, scheduledAtUtc }`
- `GET /api/message-jobs/{jobId}/progress` — First checks the in-memory `IBulkPublishProgressStore`; on cache miss falls back to reading `MessagePublishJob` from the database. Returns `BulkPublishProgress` with total, published, failed counts and completion flag
- `POST /api/message-jobs/{jobId}/cancel` — Signals cancellation to in-flight jobs via `ICancellationRegistry`
- `POST /api/message-jobs/{jobId}/retry` — Creates a retry job for failed messages
- Future: `GET /api/message-jobs/{jobId}` for full job details with status and timestamps

**Key files:** `MessageJobsController.cs`, `BulkPublishRequest.cs`, `BulkPublishResponse.cs`, `BulkPublishProgress.cs`

---

### Phase 3 — Database Schema ⬜

**Goal:** Persist job lifecycle, failure details, and audit logs.

**What needs to be done:**

- **`MessagePublishJob`** — Primary table. Tracks `JobId`, `MessageCount`, `BatchSize`, `MaxParallelPublishes`, `PayloadTemplate`, `Status` (Queued / Scheduled / Running / Completed / CompletedWithFailures / Cancelled), timestamps, and counter fields `PublishedMessages` / `FailedMessages`
- **`FailedMessage`** — Records individual publish failures with `JobId`, `SequenceNumber`, `Payload`, `Error`, and `FailedAtUtc`. Indexed on `(JobId, SequenceNumber)` for efficient replay queries
- **`JobExecutionLog`** — Append-only audit trail per job with `Level` (Information/Warning/Error) and `Message`. Indexed on `JobId`
- **`TickerQJob`** — TickerQ-managed table to track job queue entries (auto-created by TickerQ, but schema integration noted here)
- `OrchestratorDbContext` applies column constraints: `PayloadTemplate` max 4000, `Error` max 2048, `Status` max 64

**Key files:** `OrchestratorDbContext.cs`, `MessagePublishJob.cs`, `FailedMessage.cs`, `JobExecutionLog.cs`, TickerQ migration files

---

### Phase 4 — TickerQ Integration ⬜

**Goal:** Configure TickerQ as the background job orchestrator.

**What needs to be done:**

- Install TickerQ NuGet package
- Register TickerQ in `Program.cs`:
  ```csharp
  services.AddTickerQ(opts => {
      opts.JobStore = new EntityFrameworkJobStore(serviceProvider.GetRequiredService<OrchestratorDbContext>());
      opts.UsePolling(TimeSpan.FromSeconds(5)); // Poll every 5 seconds for new jobs
  });
  ```
- Create job handler definitions for:
  - **`BulkPublishJobHandler`** — Executes `BulkPublishingEngine.ExecuteAsync(jobId)` when a bulk publish job is dequeued
  - **`RetryFailedJobHandler`** — Executes `BulkPublishingEngine.RetryFailedAsync(sourceJobId)` for retry jobs
- Wire job definitions in DI: `services.AddTransient<BulkPublishJobHandler>()`, etc.
- Configure automatic retry: TickerQ retries failed jobs based on exponential backoff (configurable)
- Ensure graceful job acquisition and completion tracking via TickerQ's `IJobQueue` interface

**Key files:** `Program.cs`, `BulkPublishJobHandler.cs`, `RetryFailedJobHandler.cs`, `TickerQOptions.cs`

---

### Phase 5 — Scheduling System ⬜

**Goal:** Support recurring publish schedules driven by configuration.

**What needs to be done:**

- `RecurringScheduleOptions` — Extended with `MessageCount`, `BatchSize`, `MaxParallelPublishes`, and `PayloadTemplate` per schedule entry alongside `Id`, `Cron`, and `Enabled`
- On startup, `Program.cs` reads `RecurringSchedules` and registers a background timer service that checks schedules and enqueues jobs via TickerQ
- **`ScheduledBulkPublishingService`** (background service) — Polls configuration on startup and at regular intervals; if a schedule's cron expression matches, creates a `MessagePublishJob` entity using the schedule's parameters (falling back to global `BulkPublishingOptions` defaults), initializes the progress store, enqueues the job via TickerQ's `IJobQueue.EnqueueAsync()`, and saves the record to the database
- `appsettings.json` updated with full schedule examples including `MessageCount`, `BatchSize`, and `PayloadTemplate`
- Track last execution time per schedule to avoid duplicate enqueues

**Key files:** `ScheduledBulkPublishingService.cs`, `RecurringScheduleOptions.cs`, `Program.cs`

---

### Phase 6 — MassTransit Integration ⬜

**Goal:** Abstract message transport behind a unified MassTransit pipeline.

**What needs to be done:**

- MassTransit registered globally with `AddMassTransit`
- **Retry policy**: `UseMessageRetry(retry => retry.Interval(3, TimeSpan.FromSeconds(2)))` — retries 3 times with 2-second intervals on publish failures
- **In-memory outbox**: `UseInMemoryOutbox(context)` — prevents duplicate publishes during retries by buffering messages until the ambient transaction commits
- **Contract**: `BulkMessagePublished(JobId, SequenceNumber, Payload, PublishedAtUtc)` — the message type published per message in a job
- Transport selection is transparent to `BulkPublishingEngine`; it uses `IPublishEndpoint` regardless of backend

**Key files:** `BulkMessagePublished.cs`, `Program.cs`

---

### Phase 7 — Background Processing Engine ⬜

**Goal:** Publish 100,000+ messages without blocking the API or exhausting memory.

**What needs to be done:**

- `BulkPublishingEngine.ExecuteAsync(jobId, cancellationToken)` is the main processing method (called by TickerQ job handler)
- On start: loads the job from DB, sets `Status = "Running"`, records a start log entry
- **Chunking**: `Enumerable.Range(1, job.MessageCount).Chunk(job.BatchSize)` splits the full sequence into fixed-size batches
- **Parallelism**: `Parallel.ForEachAsync` with `MaxDegreeOfParallelism = job.MaxParallelPublishes` processes each chunk concurrently
- Per-message: formats payload via `string.Format(job.PayloadTemplate, sequence)`, publishes `BulkMessagePublished` via MassTransit. Thread-safe counters use `Interlocked.Increment`
- Per-batch: bulk-saves failed messages, updates DB counters, updates progress store, pushes SignalR event
- On finish: sets `Status = "Completed"` or `"CompletedWithFailures"`, writes final log, pushes terminal SignalR event
- Gracefully handle `OperationCanceledException` from TickerQ's provided cancellation token

**Key files:** `BulkPublishingEngine.cs`, `IBulkPublishingEngine.cs`

---

### Phase 8 — Progress Tracking ⬜

**Goal:** Provide fast, thread-safe progress reads without hitting the database on every poll.

**What needs to be done:**

- `IBulkPublishProgressStore` defines `Initialize`, `Get`, and `Update` operations
- `InMemoryBulkPublishProgressStore` stores a `ConcurrentDictionary<Guid, BulkPublishProgress>`, using `AddOrUpdate` for lock-free increment of published/failed deltas
- `BulkPublishProgress` is an immutable record: `(JobId, TotalMessages, PublishedMessages, FailedMessages, IsCompleted)`
- `GetProgressAsync` endpoint first checks the in-memory store (fast path for active jobs), then falls back to the database (for historical jobs or after restart)
- Registered as `Singleton` in DI so the store survives across scoped engine executions

**Key files:** `InMemoryBulkPublishProgressStore.cs`, `IBulkPublishProgressStore.cs`, `BulkPublishProgress.cs`

---

### Phase 9 — SignalR Integration ⬜

**Goal:** Push real-time progress events to frontend clients without polling.

**What needs to be done:**

- `ProgressHub : Hub` registered at `/hubs/progress`
- `SubscribeToJob(Guid jobId)` — client calls this to join a SignalR group keyed by `jobId.ToString()`
- After each batch, `BulkPublishingEngine` calls `progressHub.Clients.Group(jobId.ToString()).SendAsync("progress-updated", progress)` with the current `BulkPublishProgress`
- Final `progress-updated` event is sent with `IsCompleted = true` to signal job completion to all subscribers
- Clients can render a live progress bar or dashboard without any polling

**Key files:** `ProgressHub.cs`, `BulkPublishingEngine.cs`

---

### Phase 10 — Failure Handling & Retry ⬜

**Goal:** Capture, persist, and enable replay of failed messages.

**What needs to be done:**

- Per-batch failure collection with `List<FailedMessage>` (thread-safe via `lock`)
- Failed messages bulk-saved to `FailedMessages` table with error details and sequence number
- `job.FailedMessages` counter incremented and persisted after each batch
- Job status set to `"CompletedWithFailures"` when `totalFailed > 0`
- MassTransit-level retry active (3 attempts before a failure is counted)
- `RetryFailedAsync(Guid sourceJobId)` added to `IBulkPublishingEngine` and implemented in `BulkPublishingEngine`: loads all `FailedMessages` for the job, re-publishes them in parallel (max 8 concurrent), removes successfully retried entries from the table, writes an audit log
- `POST /api/message-jobs/{jobId}/retry` endpoint creates a dedicated retry `MessagePublishJob` and enqueues `RetryFailedJobHandler` via TickerQ's `IJobQueue`

**Key files:** `BulkPublishingEngine.cs`, `IBulkPublishingEngine.cs`, `MessageJobsController.cs`, `FailedMessage.cs`

---

### Phase 11 — Monitoring & Observability ⬜

**Goal:** Give operators visibility into job health, queue depth, and system performance.

**What needs to be done:**

- **Admin Dashboard** (custom or leveraging TickerQ's dashboard if available) — shows queued jobs, job status, execution history, failed jobs
- `AddHealthChecks().AddDbContextCheck<OrchestratorDbContext>()` — `/health` endpoint reflects database connectivity
- TickerQ health check: ensure job queue is responsive and processing jobs
- Structured logging via `ILogger<T>` throughout the engine and job handlers
- `ApiKeyAuthenticationHandler` — validates `X-Api-Key` header against `ApiKey` config value; allows open access when key is empty (development mode)
- Admin dashboard protected by the same API key as the REST endpoints
- Expose job queue metrics (queue depth, jobs in-flight, error rate) via health endpoint or custom metrics endpoint

**Key files:** `Program.cs`, `BulkPublishingEngine.cs`, `Auth/ApiKeyAuthenticationHandler.cs`, custom admin controller if needed

---

### Phase 12 — Security & Production Hardening ⬜

**Goal:** Secure all surfaces before production deployment.

**What needs to be done:**

- `ApiKeyAuthenticationHandler` — custom `AuthenticationHandler<AuthenticationSchemeOptions>` that validates the `X-Api-Key` request header against the `ApiKey` configuration value; passes all requests through transparently when `ApiKey` is empty (development mode)
- `[Authorize]` applied to `MessageJobsController` — all endpoints require authentication
- Fixed-window rate limiter registered via `AddRateLimiter`: 60 requests/minute on `POST /api/message-jobs` (`[EnableRateLimiting("create-job")]`); returns `429 Too Many Requests` when exceeded
- Admin dashboard protected by the same API key
- `ApiKey` configuration key added to `appsettings.json` (empty string = open dev mode, set to a secret value in production via environment variable or Key Vault)
- Ensure TickerQ job definitions validate input and sanitize payloads

**Key files:** `Auth/ApiKeyAuthenticationHandler.cs`, `MessageJobsController.cs`, `Program.cs`, `appsettings.json`

---

### Phase 13 — Deployment & Scaling ⬜

**Goal:** Deploy to Azure with horizontal scalability for the background worker.

**What needs to be done:**

- **`Dockerfile`** — multi-stage build: `sdk:10.0` for restore/build/publish, `aspnet:10.0` for the final runtime image; exposes port 8080
- **`.dockerignore`** — excludes `.git`, `bin`, `obj`, `.vs`, and docs from the build context
- **`.github/workflows/ci.yml`** — GitHub Actions pipeline with two jobs:
  - `build-and-test`: restores, builds in Release, runs tests with code coverage collection on Ubuntu
  - `docker`: builds the Docker image (tagged with commit SHA) only after tests pass
- Ensure TickerQ is configured to run as a background service within the same application (polling model), or deploy a separate TickerQ worker service for high-scale scenarios

**Key files:** `Dockerfile`, `.dockerignore`, `.github/workflows/ci.yml`

---

### Phase 14 — Future Enhancements 🔮

**Goal:** Extend the platform beyond its current scope.

| Feature                          | Description                                                                                       |
| -------------------------------- | ------------------------------------------------------------------------------------------------- |
| Distributed TickerQ worker fleet | Deploy multiple TickerQ workers across containers for massive parallelism                         |
| Redis progress store             | Replace `InMemoryBulkPublishProgressStore` with Redis to share state across API instances         |
| Transactional outbox             | Use MassTransit Entity Framework outbox for full at-least-once delivery guarantees                |
| Message replay UI                | Frontend UI to view, filter, and replay failed messages from the `FailedMessages` table           |
| Multi-tenant scheduling          | Scope recurring schedules and job history per tenant with row-level security                      |
| Analytics dashboard              | Aggregate job history, throughput trends, and failure rates in a reporting dashboard              |
| Dynamic schedule management      | REST API to create, update, pause, and delete recurring schedules at runtime without redeployment |
| Cancellation support (enhanced)  | Allow in-flight jobs to be cancelled via `POST /api/message-jobs/{jobId}/cancel` with TickerQ     |

---

## High-Level Architecture (TickerQ Version)

```
┌────────────────────────────────────────────────────────────┐
│ FRONTEND                                                   │
│────────────────────────────────────────────────────────────│
│ Blazor / React / Angular                                   │
│                                                             │
│ - Start bulk publish                                       │
│ - Show realtime progress bar                               │
│ - Receive SignalR updates                                  │
└───────────────────────┬────────────────────────────────────┘
                        │ HTTPS
                        ▼
┌────────────────────────────────────────────────────────────┐
│ ASP.NET CORE API                                           │
│────────────────────────────────────────────────────────────│
│                                                             │
│ POST /api/message-jobs                                     │
│ GET /api/message-jobs/{id}                                 │
│                                                             │
│ Responsibilities:                                          │
│ - Validate request                                         │
│ - Create Job record                                        │
│ - Enqueue TickerQ job                                      │
│ - Return JobId                                             │
└───────────────┬──────────────────────┬────────────────────┘
                │                      │
                ▼                      ▼
    ┌──────────────────────┐  ┌───────────────────────────────┐
    │ SQL SERVER / EF CORE │  │ SIGNALR HUB                   │
    │──────────────────────│  │───────────────────────────────│
    │ MessagePublishJobs   │  │ Live progress updates         │
    │ FailedMessages       │  │                               │
    │ JobExecutionLogs     │  │ UI progress synchronization   │
    │ TickerQJobs (queue)  │  │                               │
    └──────────────┬───────┘  └───────────────────────────────┘
                   │
                   ▼
    ┌────────────────────────────────────────────────────────┐
    │ TICKERQ JOB QUEUE                                      │
    │────────────────────────────────────────────────────────│
    │                                                         │
    │ - Job polling (every 5 seconds)                        │
    │ - Job dequeuing                                        │
    │ - Automatic retry logic                                │
    │ - In-process execution (or distributed workers)        │
    │ - Completion tracking                                  │
    └───────────────┬────────────────────────────────────────┘
                    │
                    ▼
    ┌────────────────────────────────────────────────────────┐
    │ BACKGROUND PUBLISHER ENGINE                            │
    │────────────────────────────────────────────────────────│
    │                                                         │
    │ Read Source Data                                       │
    │     │                                                  │
    │     ▼                                                  │
    │ Chunk Data (1000)                                      │
    │     │                                                  │
    │     ▼                                                  │
    │ Parallel Processing                                    │
    │ MaxDegreeOfParallelism = 10                            │
    │     │                                                  │
    │     ▼                                                  │
    │ MassTransit Publish                                    │
    │     │                                                  │
    │     ▼                                                  │
    │ Update Progress + SignalR                              │
    └───────────────┬────────────────────────────────────────┘
                    │
                    ▼
    ┌────────────────────────────────────────────────────────┐
    │ MASSTRANSIT                                            │
    │────────────────────────────────────────────────────────│
    │                                                         │
    │ - Retry policies                                       │
    │ - Serialization                                        │
    │ - Message abstraction                                  │
    │ - Outbox support                                       │
    │ - Consumer pipeline                                    │
    └───────────────┬────────────────────────────────────────┘
                    │
                    ▼
    ┌────────────────────────────────────────────────────────┐
    │ AZURE SERVICE BUS                                      │
    │────────────────────────────────────────────────────────│
    │                                                         │
    │ Queue / Topic                                          │
    │                                                         │
    │ Receives 100000+ messages                              │
    │                                                         │
    │ - Durable messaging                                    │
    │ - Dead-letter queue                                    │
    │ - High throughput                                      │
    │ - Retry support                                        │
    └───────────────┬────────────────────────────────────────┘
                    │
                    ▼
    ┌────────────────────────────────────────────────────────┐
    │ DOWNSTREAM CONSUMERS                                   │
    │────────────────────────────────────────────────────────│
    │                                                         │
    │ - Notification Service                                 │
    │ - Billing Service                                      │
    │ - Reporting Service                                    │
    │ - Audit Service                                        │
    │                                                         │
    └────────────────────────────────────────────────────────┘
```

---

## End-to-End Execution Flow

```
API Request: POST /api/message-jobs
│
▼
Create MessagePublishJob record
│
▼
Enqueue job via TickerQ.IJobQueue
│
▼
Return 202 Accepted
│
▼
TickerQ polling detects new job
│
▼
Dequeue and invoke BulkPublishJobHandler
│
▼
BulkPublishingEngine.ExecuteAsync starts
│
▼
Read Message Count
│
▼
Chunk Messages (batch size)
│
▼
Parallel MassTransit Publish
│
├── Update DB Progress
│
├── Push SignalR Updates
│
└── Handle Failures (persist to FailedMessages)
│
▼
Job Completion Handler marks TickerQ job complete
│
▼
Job Completed
```

---

## Key Differences from Hangfire Implementation

| Aspect             | Hangfire                                                | TickerQ                                                           |
| ------------------ | ------------------------------------------------------- | ----------------------------------------------------------------- |
| **Storage**        | Separate Hangfire database or SQL Server                | Application's EF Core DbContext                                   |
| **Server**         | Separate Hangfire Server process                        | In-process polling (or separate worker for scale)                 |
| **Job Scheduling** | `BackgroundJob.Enqueue()`, `RecurringJob.AddOrUpdate()` | `IJobQueue.EnqueueAsync()`, custom polling service for recurrence |
| **Retry Logic**    | Built-in retry with exponential backoff                 | Custom or TickerQ's built-in exponential backoff                  |
| **Dashboard**      | Rich built-in Hangfire Dashboard                        | Custom dashboard or TickerQ's provided UI (if available)          |
| **Infrastructure** | Requires Hangfire Server instance                       | Embedded or distributed worker services                           |
| **Testing**        | Easier with in-memory storage                           | Native to EF in-memory database (same as app DB)                  |

---

## Recommended Production Deployment

```
Azure App Service +
Azure SQL (single DB for app + TickerQ) +
Azure Service Bus +
[Optional] Separate TickerQ Worker Service (for scale) +
Application Insights +
SignalR
```

---

## Suggested Initial Milestone Order

| Milestone                | Priority |
| ------------------------ | -------- |
| API + DB + TickerQ setup | High     |
| MassTransit              | High     |
| Progress Tracking        | High     |
| Background Engine        | High     |
| SignalR                  | Medium   |
| Retry Handling           | Medium   |
| Monitoring               | Medium   |
| Scaling Optimization     | Later    |

---

## Migration Considerations from Hangfire

If migrating from an existing Hangfire implementation:

1. **Job Store**: Move job metadata from Hangfire's storage to `MessagePublishJob` and `TickerQJob` tables
2. **Job Definitions**: Convert `BackgroundJob.Enqueue()` calls to `IJobQueue.EnqueueAsync()` calls
3. **Recurring Jobs**: Replace `RecurringJob.AddOrUpdate()` with `ScheduledBulkPublishingService` polling
4. **Testing**: Leverage EF in-memory database for TickerQ job testing
5. **Dashboard**: Replace Hangfire Dashboard with custom admin UI or TickerQ's dashboard
6. **Monitoring**: Adapt monitoring queries to use `TickerQJob` table instead of Hangfire tables
