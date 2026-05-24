# Bulk Message Orchestrator — Implementation Plan

## Overview

**Bulk Message Orchestrator** is an ASP.NET Core Web API that orchestrates large-scale message publishing (100,000+ messages) to Azure Service Bus via MassTransit. It uses Hangfire for durable background processing and scheduled jobs, Entity Framework Core for persistence, and SignalR for real-time progress streaming to connected clients.

The system is designed to:

- Accept a publish request via HTTP and return immediately with a `JobId`
- Enqueue or schedule a Hangfire background job to do the heavy lifting
- Chunk messages into batches and publish them in parallel via MassTransit
- Persist job state, failed messages, and execution logs to SQL Server
- Push live progress updates to subscribed clients through a SignalR hub
- Fall back gracefully to in-memory transports and databases for local development

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
| Phase 1 — Project Foundation               | ✅     | Initialize core infrastructure       | ASP.NET Core Web API bootstrapped with dual-mode infrastructure: SQL Server or EF in-memory DB, Hangfire with SQL or memory storage, MassTransit with Azure Service Bus or in-memory transport.                                |
| Phase 2 — API Design                       | ✅     | Create bulk publishing endpoints     | `POST /api/message-jobs`, `GET /{id}/progress`, `POST /{id}/cancel`, and `POST /{id}/retry` endpoints. Returns `202 Accepted` with `JobId` on creation.                                                                        |
| Phase 3 — Database Schema                  | ✅     | Design persistence layer             | EF Core entities for `MessagePublishJob`, `FailedMessage`, and `JobExecutionLog`. DbContext configured with constraints, indexes, and SQL Server/in-memory support.                                                             |
| Phase 4 — Scheduling System                | ✅     | Implement configurable scheduling    | `RecurringScheduleOptions` extended with `MessageCount`, `BatchSize`, `MaxParallelPublishes`, `PayloadTemplate`. `ScheduledBulkPublishingJob.ExecuteAsync(schedule)` creates a job record and enqueues processing via Hangfire. |
| Phase 5 — MassTransit Integration          | ✅     | Configure messaging infrastructure   | MassTransit registered with retry policy (3 attempts, 2-second interval), in-memory outbox, and Azure Service Bus or in-memory transport. `BulkMessagePublished` contract defined.                                             |
| Phase 6 — Background Processing Engine     | ✅     | Build scalable publisher             | `BulkPublishingEngine` chunks messages by `BatchSize`, publishes in parallel via `Parallel.ForEachAsync` with configurable `MaxDegreeOfParallelism`, and persists progress after each batch.                                   |
| Phase 7 — Progress Tracking               | ✅     | Enable real-time progress visibility | `InMemoryBulkPublishProgressStore` uses `ConcurrentDictionary` to track total, published, failed, and completion state per job. Falls back to DB for jobs not in memory.                                                        |
| Phase 8 — SignalR Integration              | ✅     | Add real-time client updates         | `ProgressHub` allows clients to subscribe to a job group. The engine pushes `progress-updated` events after every batch and on completion.                                                                                     |
| Phase 9 — Failure Handling & Retry         | ✅     | Improve reliability                  | Failed messages persisted to `FailedMessages`. `POST /{id}/retry` creates a retry job. `RetryFailedAsync` re-publishes failures, removes succeeded entries. MassTransit retry policy active.                                   |
| Phase 10 — Monitoring & Observability      | ✅     | Add operational visibility           | Hangfire Dashboard protected by `HangfireDashboardAuthorizationFilter`. Health check at `/health`. Structured logging throughout. Open when `ApiKey` is empty (dev mode).                                                      |
| Phase 11 — Performance Optimization        | ✅     | Optimize throughput and stability    | Configurable `BatchSize` and `MaxParallelPublishes` per-request and via options. Cancellation support via `ICancellationRegistry` + `POST /{id}/cancel`. Linked `CancellationTokenSource` in engine.                           |
| Phase 12 — Security & Production Hardening | ✅     | Prepare for production deployment    | `ApiKeyAuthenticationHandler` with `[Authorize]` on all endpoints. Fixed-window rate limiter (60 req/min) on create. `ApiKey` config key (empty = open dev mode). Hangfire Dashboard protected by same key.                   |
| Phase 13 — Deployment & Scaling            | ✅     | Enable cloud scalability             | Multi-stage `Dockerfile` targeting .NET 10. `.dockerignore` included. `.github/workflows/ci.yml` with restore → build → test → Docker build pipeline.                                                                         |
| Phase 14 — Future Enhancements             | 🔮     | Extend platform capabilities         | Multi-tenant scheduling, transactional outbox pattern, Redis-backed progress store, analytics dashboard, distributed worker orchestration, and message replay UI.                                                               |

---

## Detailed Phase Descriptions

### Phase 1 — Project Foundation ✅

**Goal:** Bootstrap the project with all required infrastructure wired to dual-mode transports.

**What was done:**

- ASP.NET Core Web API created with `Program.cs` as the composition root
- **Database**: If `ConnectionStrings:SqlServer` is present, EF Core uses `UseSqlServer`; otherwise falls back to `UseInMemoryDatabase("orchestrator-db")` for local development
- **Hangfire**: If `ConnectionStrings:Hangfire` is present, uses `UseSqlServerStorage`; otherwise uses `UseMemoryStorage()`. `AddHangfireServer()` registers the background worker
- **MassTransit**: If `ConnectionStrings:AzureServiceBus` is present, uses `UsingAzureServiceBus`; otherwise uses `UsingInMemory` — both configured with the same retry and outbox pipeline
- `BulkPublishingOptions` bound from `appsettings.json` under `"BulkPublishing"` (configured default batch size: 1000, max parallelism: 16, retry count: 3)
- Health checks registered for `OrchestratorDbContext`

**Key files:** `Program.cs`, `appsettings.json`, `BulkPublishingOptions.cs`

---

### Phase 2 — API Design ✅

**Goal:** Expose HTTP endpoints that decouple the caller from background processing.

**What was done:**

- `POST /api/message-jobs` — Accepts `BulkPublishRequest` (message count, optional batch size, parallelism, payload template, optional `ScheduleAtUtc`). Creates a `MessagePublishJob` record, initializes the progress store, enqueues or schedules a Hangfire job, and returns `202 Accepted` with `{ jobId, hangfireJobId, scheduledAtUtc }`
- `GET /api/message-jobs/{jobId}/progress` — First checks the in-memory `IBulkPublishProgressStore`; on cache miss falls back to reading `MessagePublishJob` from the database. Returns `BulkPublishProgress` with total, published, failed counts and completion flag
- Future: `GET /api/message-jobs/{jobId}` for full job details, `POST /api/message-jobs/{jobId}/retry` for replaying failed messages

**Key files:** `MessageJobsController.cs`, `BulkPublishRequest.cs`, `BulkPublishResponse.cs`, `BulkPublishProgress.cs`

---

### Phase 3 — Database Schema ✅

**Goal:** Persist job lifecycle, failure details, and audit logs.

**What was done:**

- **`MessagePublishJob`** — Primary table. Tracks `JobId`, `MessageCount`, `BatchSize`, `MaxParallelPublishes`, `PayloadTemplate`, `Status` (Queued / Scheduled / Running / Completed / CompletedWithFailures), timestamps, and counter fields `PublishedMessages` / `FailedMessages`
- **`FailedMessage`** — Records individual publish failures with `JobId`, `SequenceNumber`, `Payload`, `Error`, and `FailedAtUtc`. Indexed on `(JobId, SequenceNumber)` for efficient replay queries
- **`JobExecutionLog`** — Append-only audit trail per job with `Level` (Information/Warning/Error) and `Message`. Indexed on `JobId`
- `OrchestratorDbContext` applies column constraints: `PayloadTemplate` max 4000, `Error` max 2048, `Status` max 64

**Key files:** `OrchestratorDbContext.cs`, `MessagePublishJob.cs`, `FailedMessage.cs`, `JobExecutionLog.cs`

---

### Phase 4 — Scheduling System ✅

**Goal:** Support recurring publish schedules driven by configuration.

**What was done:**

- `RecurringScheduleOptions` — Extended with `MessageCount`, `BatchSize`, `MaxParallelPublishes`, and `PayloadTemplate` per schedule entry alongside `Id`, `Cron`, and `Enabled`
- On startup, `Program.cs` reads `RecurringSchedules` and calls `recurringManager.AddOrUpdate<ScheduledBulkPublishingJob>(schedule.Id, job => job.ExecuteAsync(captured), schedule.Cron, ...)`
- `ScheduledBulkPublishingJob.ExecuteAsync(schedule)` creates a `MessagePublishJob` entity using the schedule's parameters (falling back to global `BulkPublishingOptions` defaults), initializes the progress store, enqueues `IBulkPublishingEngine` via Hangfire, and saves the record to the database
- `appsettings.json` updated with full schedule examples including `MessageCount`, `BatchSize`, and `PayloadTemplate`

**Key files:** `ScheduledBulkPublishingJob.cs`, `RecurringScheduleOptions.cs`, `Program.cs`

---

### Phase 5 — MassTransit Integration ✅

**Goal:** Abstract message transport behind a unified MassTransit pipeline.

**What was done:**

- MassTransit registered globally with `AddMassTransit`
- **Retry policy**: `UseMessageRetry(retry => retry.Interval(3, TimeSpan.FromSeconds(2)))` — retries 3 times with 2-second intervals on publish failures
- **In-memory outbox**: `UseInMemoryOutbox(context)` — prevents duplicate publishes during retries by buffering messages until the ambient transaction commits
- **Contract**: `BulkMessagePublished(JobId, SequenceNumber, Payload, PublishedAtUtc)` — the message type published per message in a job
- Transport selection is transparent to `BulkPublishingEngine`; it uses `IPublishEndpoint` regardless of backend

**Key files:** `BulkMessagePublished.cs`, `Program.cs`

---

### Phase 6 — Background Processing Engine ✅

**Goal:** Publish 100,000+ messages without blocking the API or exhausting memory.

**What was done:**

- `BulkPublishingEngine.ExecuteAsync(jobId)` is the Hangfire job body
- On start: loads the job from DB, sets `Status = "Running"`, records a start log entry
- **Chunking**: `Enumerable.Range(1, job.MessageCount).Chunk(job.BatchSize)` splits the full sequence into fixed-size batches
- **Parallelism**: `Parallel.ForEachAsync` with `MaxDegreeOfParallelism = job.MaxParallelPublishes` processes each chunk concurrently
- Per-message: formats payload via `string.Format(job.PayloadTemplate, sequence)`, publishes `BulkMessagePublished` via MassTransit. Thread-safe counters use `Interlocked.Increment`
- Per-batch: bulk-saves failed messages, updates DB counters, updates progress store, pushes SignalR event
- On finish: sets `Status = "Completed"` or `"CompletedWithFailures"`, writes final log, pushes terminal SignalR event

**Key files:** `BulkPublishingEngine.cs`, `IBulkPublishingEngine.cs`

---

### Phase 7 — Progress Tracking ✅

**Goal:** Provide fast, thread-safe progress reads without hitting the database on every poll.

**What was done:**

- `IBulkPublishProgressStore` defines `Initialize`, `Get`, and `Update` operations
- `InMemoryBulkPublishProgressStore` stores a `ConcurrentDictionary<Guid, BulkPublishProgress>`, using `AddOrUpdate` for lock-free increment of published/failed deltas
- `BulkPublishProgress` is an immutable record: `(JobId, TotalMessages, PublishedMessages, FailedMessages, IsCompleted)`
- `GetProgressAsync` endpoint first checks the in-memory store (fast path for active jobs), then falls back to the database (for historical jobs or after restart)
- Registered as `Singleton` in DI so the store survives across scoped engine executions

**Key files:** `InMemoryBulkPublishProgressStore.cs`, `IBulkPublishProgressStore.cs`, `BulkPublishProgress.cs`

---

### Phase 8 — SignalR Integration ✅

**Goal:** Push real-time progress events to frontend clients without polling.

**What was done:**

- `ProgressHub : Hub` registered at `/hubs/progress`
- `SubscribeToJob(Guid jobId)` — client calls this to join a SignalR group keyed by `jobId.ToString()`
- After each batch, `BulkPublishingEngine` calls `progressHub.Clients.Group(jobId.ToString()).SendAsync("progress-updated", progress)` with the current `BulkPublishProgress`
- Final `progress-updated` event is sent with `IsCompleted = true` to signal job completion to all subscribers
- Clients can render a live progress bar or dashboard without any polling

**Key files:** `ProgressHub.cs`, `BulkPublishingEngine.cs`

---

### Phase 9 — Failure Handling & Retry ✅

**Goal:** Capture, persist, and enable replay of failed messages.

**What was done:**

- Per-batch failure collection with `List<FailedMessage>` (thread-safe via `lock`)
- Failed messages bulk-saved to `FailedMessages` table with error details and sequence number
- `job.FailedMessages` counter incremented and persisted after each batch
- Job status set to `"CompletedWithFailures"` when `totalFailed > 0`
- MassTransit-level retry active (3 attempts before a failure is counted)
- `RetryFailedAsync(Guid sourceJobId)` added to `IBulkPublishingEngine` and implemented in `BulkPublishingEngine`: loads all `FailedMessages` for the job, re-publishes them in parallel (max 8 concurrent), removes successfully retried entries from the table, writes an audit log
- `POST /api/message-jobs/{jobId}/retry` endpoint creates a dedicated retry `MessagePublishJob` and enqueues `RetryFailedAsync` via Hangfire

**Key files:** `BulkPublishingEngine.cs`, `IBulkPublishingEngine.cs`, `MessageJobsController.cs`, `FailedMessage.cs`

---

### Phase 10 — Monitoring & Observability ✅

**Goal:** Give operators visibility into job health, queue depth, and system performance.

**What was done:**

- Hangfire Dashboard mapped at `/hangfire` — shows job queues, retry counts, succeeded/failed job history
- `AddHealthChecks().AddDbContextCheck<OrchestratorDbContext>()` — `/health` endpoint reflects database connectivity
- Structured logging via `ILogger<T>` throughout the engine and job classes
- `HangfireDashboardAuthorizationFilter` — validates `X-Api-Key` header against `ApiKey` config value; allows open access when key is empty (development mode)
- Dashboard protected by the same API key as the REST endpoints

**Key files:** `Program.cs`, `BulkPublishingEngine.cs`, `Auth/HangfireDashboardAuthorizationFilter.cs`

---

### Phase 11 — Performance Optimization ✅

**Goal:** Sustain high throughput under large-scale loads without memory pressure or DB bottlenecks.

**What was done:**

- `BatchSize` (configured default 1000) and `MaxParallelPublishes` (configured default 16) configurable globally via `BulkPublishingOptions` and overridable per-request
- `Interlocked` for lock-free counter updates during parallel processing
- DB writes are batched per chunk (one `SaveChangesAsync` per batch, not per message)
- In-memory progress store avoids DB reads on progress polls
- **Cancellation support**: `ICancellationRegistry` / `InMemoryCancellationRegistry` — registers a `CancellationTokenSource` per job; `BulkPublishingEngine` creates a linked token combining the Hangfire-provided token and the registry token
- `POST /api/message-jobs/{jobId}/cancel` signals cancellation; engine catches `OperationCanceledException`, sets `Status = "Cancelled"`, writes audit log
- `OperationCanceledException` is re-thrown correctly inside `Parallel.ForEachAsync` to propagate cleanly

**Key files:** `BulkPublishingEngine.cs`, `BulkPublishingOptions.cs`, `InMemoryBulkPublishProgressStore.cs`, `ICancellationRegistry.cs`, `InMemoryCancellationRegistry.cs`, `MessageJobsController.cs`

---

### Phase 12 — Security & Production Hardening ✅

**Goal:** Secure all surfaces before production deployment.

**What was done:**

- `ApiKeyAuthenticationHandler` — custom `AuthenticationHandler<AuthenticationSchemeOptions>` that validates the `X-Api-Key` request header against the `ApiKey` configuration value; passes all requests through transparently when `ApiKey` is empty (development mode)
- `[Authorize]` applied to `MessageJobsController` — all endpoints require authentication
- Fixed-window rate limiter registered via `AddRateLimiter`: 60 requests/minute on `POST /api/message-jobs` (`[EnableRateLimiting("create-job")]`); returns `429 Too Many Requests` when exceeded
- `HangfireDashboardAuthorizationFilter` protects the Hangfire Dashboard with the same API key
- `ApiKey` configuration key added to `appsettings.json` (empty string = open dev mode, set to a secret value in production via environment variable or Key Vault)

**Key files:** `Auth/ApiKeyAuthenticationHandler.cs`, `Auth/HangfireDashboardAuthorizationFilter.cs`, `MessageJobsController.cs`, `Program.cs`, `appsettings.json`

---

### Phase 13 — Deployment & Scaling ✅

**Goal:** Deploy to Azure with horizontal scalability for the background worker.

**What was done:**

- **`Dockerfile`** — multi-stage build: `sdk:10.0` for restore/build/publish, `aspnet:10.0` for the final runtime image; exposes port 8080
- **`.dockerignore`** — excludes `.git`, `bin`, `obj`, `.vs`, and docs from the build context
- **`.github/workflows/ci.yml`** — GitHub Actions pipeline with two jobs:
  - `build-and-test`: restores, builds in Release, runs tests with code coverage collection on Ubuntu
  - `docker`: builds the Docker image (tagged with commit SHA) only after tests pass

**Key files:** `Dockerfile`, `.dockerignore`, `.github/workflows/ci.yml`

---

### Phase 14 — Future Enhancements 🔮

**Goal:** Extend the platform beyond its current scope.

| Feature                     | Description                                                                                       |
| --------------------------- | ------------------------------------------------------------------------------------------------- |
| Redis progress store        | Replace `InMemoryBulkPublishProgressStore` with Redis to share state across API instances         |
| Transactional outbox        | Use MassTransit Entity Framework outbox for full at-least-once delivery guarantees                |
| Message replay UI           | Frontend UI to view, filter, and replay failed messages from the `FailedMessages` table           |
| Multi-tenant scheduling     | Scope recurring schedules and job history per tenant with row-level security                      |
| Analytics dashboard         | Aggregate job history, throughput trends, and failure rates in a reporting dashboard              |
| Distributed worker fleet    | Fan-out Hangfire jobs across a pool of dedicated worker containers for massive parallelism        |
| Dynamic schedule management | REST API to create, update, pause, and delete recurring schedules at runtime without redeployment |
| Cancellation support        | Allow in-flight jobs to be cancelled via `POST /api/message-jobs/{jobId}/cancel`                  |

---

## High-Level Architecture

┌────────────────────────────────────────────────────────────┐
│ FRONTEND │
│────────────────────────────────────────────────────────────│
│ Blazor / React / Angular │
│ │
│ - Start bulk publish │
│ - Show realtime progress bar │
│ - Receive SignalR updates │
└───────────────────────┬────────────────────────────────────┘
│ HTTPS
▼
┌────────────────────────────────────────────────────────────┐
│ ASP.NET CORE API │
│────────────────────────────────────────────────────────────│
│ │
│ POST /api/message-jobs │
│ GET /api/message-jobs/{id} │
│ │
│ Responsibilities: │
│ - Validate request │
│ - Create Job record │
│ - Enqueue Hangfire job │
│ - Return JobId │
└───────────────┬──────────────────────┬────────────────────┘
│ │
▼ ▼
┌──────────────────────┐ ┌───────────────────────────────┐
│ SQL SERVER │ │ SIGNALR HUB │
│──────────────────────│ │───────────────────────────────│
│ MessagePublishJobs │ │ Live progress updates │
│ FailedMessages │ │ │
│ JobExecutionLogs │ │ UI progress synchronization │
└──────────────┬───────┘ └───────────────────────────────┘
│
▼
┌────────────────────────────────────────────────────────────┐
│ HANGFIRE │
│────────────────────────────────────────────────────────────│
│ │
│ - Recurring jobs │
│ - Cron scheduling │
│ - Retry management │
│ - Dashboard monitoring │
│ - Background orchestration │
└───────────────┬────────────────────────────────────────────┘
│
▼
┌────────────────────────────────────────────────────────────┐
│ BACKGROUND PUBLISHER │
│────────────────────────────────────────────────────────────│
│ │
│ Read Source Data │
│ │ │
│ ▼ │
│ Chunk Data (1000) │
│ │ │
│ ▼ │
│ Parallel Processing │
│ MaxDegreeOfParallelism = 10 │
│ │ │
│ ▼ │
│ MassTransit Publish │
│ │ │
│ ▼ │
│ Update Progress + SignalR │
└───────────────┬────────────────────────────────────────────┘
│
▼
┌────────────────────────────────────────────────────────────┐
│ MASSTRANSIT │
│────────────────────────────────────────────────────────────│
│ │
│ - Retry policies │
│ - Serialization │
│ - Message abstraction │
│ - Outbox support │
│ - Consumer pipeline │
└───────────────┬────────────────────────────────────────────┘
│
▼
┌────────────────────────────────────────────────────────────┐
│ AZURE SERVICE BUS │
│────────────────────────────────────────────────────────────│
│ │
│ Queue / Topic │
│ │
│ Receives 100000+ messages │
│ │
│ - Durable messaging │
│ - Dead-letter queue │
│ - High throughput │
│ - Retry support │
└───────────────┬────────────────────────────────────────────┘
│
▼
┌────────────────────────────────────────────────────────────┐
│ DOWNSTREAM CONSUMERS │
│────────────────────────────────────────────────────────────│
│ │
│ - Notification Service │
│ - Billing Service │
│ - Reporting Service │
│ - Audit Service │
│ │
└────────────────────────────────────────────────────────────┘

## End-to-End Execution Flow

Scheduler Trigger
│
▼
Hangfire Job Starts
│
▼
Create Job Record
│
▼
Read Data Source
│
▼
Chunk Messages
│
▼
Parallel MassTransit Publish
│
├── Update DB Progress
│
├── Push SignalR Updates
│
└── Handle Failures
│
▼
Job Completed

## Recommended Production Deployment

Azure App Service +
Azure SQL +
Azure Service Bus +
Hangfire +
Application Insights +
SignalR

## Suggested Initial Milestone Order

| Milestone            | Priority |
| -------------------- | -------- |
| API + DB             | High     |
| Hangfire             | High     |
| MassTransit          | High     |
| Progress Tracking    | High     |
| SignalR              | Medium   |
| Retry Handling       | Medium   |
| Monitoring           | Medium   |
| Scaling Optimization | Later    |
