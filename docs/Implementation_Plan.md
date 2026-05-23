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

| Phase                                      | Status | Objective                            | Summary                                                                                                                                                                                         |
| ------------------------------------------ | ------ | ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Phase 1 — Project Foundation               | ✅     | Initialize core infrastructure       | ASP.NET Core Web API bootstrapped with dual-mode infrastructure: SQL Server or EF in-memory DB, Hangfire with SQL or memory storage, MassTransit with Azure Service Bus or in-memory transport. |
| Phase 2 — API Design                       | ✅     | Create bulk publishing endpoints     | `POST /api/message-jobs` accepts a publish request and returns `202 Accepted` with a `JobId`. `GET /api/message-jobs/{id}/progress` returns live or persisted progress data.                    |
| Phase 3 — Database Schema                  | ✅     | Design persistence layer             | EF Core entities for `MessagePublishJob`, `FailedMessage`, and `JobExecutionLog`. DbContext configured with constraints, indexes, and SQL Server/in-memory support.                             |
| Phase 4 — Scheduling System                | 🔄     | Implement configurable scheduling    | Hangfire recurring jobs registered from `RecurringSchedules` config section with cron expressions. `ScheduledBulkPublishingJob.ExecuteAsync()` is a placeholder — job body not yet wired up.    |
| Phase 5 — MassTransit Integration          | ✅     | Configure messaging infrastructure   | MassTransit registered with retry policy (3 attempts, 2-second interval), in-memory outbox, and Azure Service Bus or in-memory transport. `BulkMessagePublished` contract defined.              |
| Phase 6 — Background Processing Engine     | ✅     | Build scalable publisher             | `BulkPublishingEngine` chunks messages by `BatchSize`, publishes in parallel via `Parallel.ForEachAsync` with configurable `MaxDegreeOfParallelism`, and persists progress after each batch.    |
| Phase 7 — Progress Tracking                | ✅     | Enable real-time progress visibility | `InMemoryBulkPublishProgressStore` uses `ConcurrentDictionary` to track total, published, failed, and completion state per job. Falls back to DB for jobs not in memory.                        |
| Phase 8 — SignalR Integration              | ✅     | Add real-time client updates         | `ProgressHub` allows clients to subscribe to a job group. The engine pushes `progress-updated` events after every batch and on completion.                                                      |
| Phase 9 — Failure Handling & Retry         | 🔄     | Improve reliability                  | Failed messages are captured per-batch and persisted to `FailedMessages`. MassTransit retry policy is active. Dead-letter replay and manual retry endpoints are not yet implemented.            |
| Phase 10 — Monitoring & Observability      | 🔄     | Add operational visibility           | Hangfire Dashboard exposed at `/hangfire`. Health check registered for `OrchestratorDbContext`. Structured logging in place. Application Insights and custom metrics not yet integrated.        |
| Phase 11 — Performance Optimization        | 🔄     | Optimize throughput and stability    | `BatchSize` (configured default 1000) and `MaxParallelPublishes` (configured default 16) are configurable per-request and via options. DB flush strategy and memory pressure handling need further tuning.              |
| Phase 12 — Security & Production Hardening | ⬜     | Prepare for production deployment    | No authentication or authorization on API endpoints or Hangfire Dashboard. Rate limiting, secret management, and resilience safeguards not yet added.                                           |
| Phase 13 — Deployment & Scaling            | ⬜     | Enable cloud scalability             | No deployment manifests or CI/CD pipelines. Needs Azure App Service or Container Apps configuration, distributed Hangfire workers, and multi-instance support.                                  |
| Phase 14 — Future Enhancements             | 🔮     | Extend platform capabilities         | Multi-tenant scheduling, transactional outbox pattern, Redis-backed progress store, analytics dashboard, distributed worker orchestration, and message replay UI.                               |

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

### Phase 4 — Scheduling System 🔄

**Goal:** Support recurring publish schedules driven by configuration.

**What was done:**
- `RecurringScheduleOptions` — Defines `Id`, `Cron`, and `Enabled` per schedule entry
- On startup, `Program.cs` reads `RecurringSchedules` from configuration and calls `recurringManager.AddOrUpdate<ScheduledBulkPublishingJob>(...)` for each enabled schedule with a valid `Id` and `Cron`
- `ScheduledBulkPublishingJob.ExecuteAsync()` is registered and invoked by Hangfire on the cron schedule

**What remains:**
- `ScheduledBulkPublishingJob.ExecuteAsync()` currently only logs a timestamp — it must be wired to create a `MessagePublishJob` record and enqueue a `BulkPublishingEngine` job, likely reading job parameters from configuration or a "schedule template" entity
- No runtime management of schedules (add/update/delete via API)
- No per-schedule payload template or message count configuration

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

### Phase 9 — Failure Handling & Retry 🔄

**Goal:** Capture, persist, and enable replay of failed messages.

**What was done:**
- Per-batch failure collection with `List<FailedMessage>` (thread-safe via `lock`)
- Failed messages bulk-saved to `FailedMessages` table with error details and sequence number
- `job.FailedMessages` counter incremented and persisted after each batch
- Job status set to `"CompletedWithFailures"` when `totalFailed > 0`
- MassTransit-level retry active (3 attempts before a failure is counted)

**What remains:**
- No `POST /api/message-jobs/{jobId}/retry` endpoint to replay stored failed messages
- No dead-letter queue integration or DLQ monitoring
- No Hangfire-level job-retry (currently a failed Hangfire job attempt would re-run the entire job, not just failed messages)
- No alerting or notification on high failure rates

**Key files:** `BulkPublishingEngine.cs`, `FailedMessage.cs`

---

### Phase 10 — Monitoring & Observability 🔄

**Goal:** Give operators visibility into job health, queue depth, and system performance.

**What was done:**
- Hangfire Dashboard mapped at `/hangfire` — shows job queues, retry counts, succeeded/failed job history
- `AddHealthChecks().AddDbContextCheck<OrchestratorDbContext>()` — `/health` endpoint reflects database connectivity
- Structured logging via `ILogger<T>` throughout the engine and job classes

**What remains:**
- No Azure Application Insights integration (`TelemetryClient`, dependency tracking, custom events)
- No custom metrics (messages/sec throughput, queue depth, failure rate)
- No distributed tracing (Activity/OpenTelemetry)
- Hangfire Dashboard has no authentication — accessible to anyone in current state
- No alerting rules or dashboards (e.g., Azure Monitor, Grafana)

**Key files:** `Program.cs`, `BulkPublishingEngine.cs`

---

### Phase 11 — Performance Optimization 🔄

**Goal:** Sustain high throughput under large-scale loads without memory pressure or DB bottlenecks.

**What was done:**
- `BatchSize` (configured default 1000) and `MaxParallelPublishes` (configured default 16) configurable globally via `BulkPublishingOptions` and overridable per-request
- `Interlocked` for lock-free counter updates during parallel processing
- DB writes are batched per chunk (one `SaveChangesAsync` per batch, not per message)
- In-memory progress store avoids DB reads on progress polls

**What remains:**
- No back-pressure mechanism — a single request can demand max concurrency immediately
- `InMemoryBulkPublishProgressStore` is not distributed; state is lost on restart or in multi-instance deployments
- No connection pool tuning for high-throughput Azure Service Bus publishing
- Chunk size not dynamically adjusted based on observed throughput or error rate
- No cancellation support for running jobs via API

**Key files:** `BulkPublishingEngine.cs`, `BulkPublishingOptions.cs`, `InMemoryBulkPublishProgressStore.cs`

---

### Phase 12 — Security & Production Hardening ⬜

**Goal:** Secure all surfaces before production deployment.

**Planned work:**
- Add JWT/OAuth2 bearer token authentication to all API controllers
- Require authenticated/authorized access to the Hangfire Dashboard (e.g., role-based policy or IP allowlist)
- Store connection strings in Azure Key Vault or environment-injected secrets — never in `appsettings.json`
- Add rate limiting middleware (e.g., `AddRateLimiter`) to prevent runaway job creation
- Add request validation middleware and `FluentValidation` for `BulkPublishRequest` (e.g., max message count cap, payload template length)
- Enable HTTPS enforcement and HSTS headers
- Add Polly-based resilience pipeline for DB operations under heavy load

**Key files to create/update:** `Program.cs`, `MessageJobsController.cs`, new auth middleware

---

### Phase 13 — Deployment & Scaling ⬜

**Goal:** Deploy to Azure with horizontal scalability for the background worker.

**Planned work:**
- Create `Dockerfile` and optionally `docker-compose.yml` for local container testing
- Create Azure Bicep or Terraform templates for: Azure App Service or Container Apps, Azure SQL, Azure Service Bus (Standard or Premium tier), Application Insights
- Configure Hangfire with `UseSqlServerStorage` and multiple worker processes to distribute job execution across instances
- Separate the Hangfire worker into a dedicated service (or use Worker Service pattern) to allow independent scaling from the API layer
- Set up GitHub Actions CI/CD pipeline: build → test → publish → deploy
- Configure environment-specific `appsettings.{Environment}.json` or Azure App Configuration

**Key files to create:** `Dockerfile`, CI/CD YAML, infrastructure-as-code templates

---

### Phase 14 — Future Enhancements 🔮

**Goal:** Extend the platform beyond its current scope.

| Feature                      | Description                                                                                        |
| ---------------------------- | -------------------------------------------------------------------------------------------------- |
| Redis progress store         | Replace `InMemoryBulkPublishProgressStore` with Redis to share state across API instances          |
| Transactional outbox         | Use MassTransit Entity Framework outbox for full at-least-once delivery guarantees                 |
| Message replay UI            | Frontend UI to view, filter, and replay failed messages from the `FailedMessages` table            |
| Multi-tenant scheduling      | Scope recurring schedules and job history per tenant with row-level security                       |
| Analytics dashboard          | Aggregate job history, throughput trends, and failure rates in a reporting dashboard               |
| Distributed worker fleet     | Fan-out Hangfire jobs across a pool of dedicated worker containers for massive parallelism          |
| Dynamic schedule management  | REST API to create, update, pause, and delete recurring schedules at runtime without redeployment  |
| Cancellation support         | Allow in-flight jobs to be cancelled via `POST /api/message-jobs/{jobId}/cancel`                  |

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
