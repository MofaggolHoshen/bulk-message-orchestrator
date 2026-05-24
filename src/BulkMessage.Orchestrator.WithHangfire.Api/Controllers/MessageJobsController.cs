using BulkMessage.Orchestrator.Api.Entities;
using BulkMessage.Orchestrator.WithHangfire.Api.Data;
using BulkMessage.Orchestrator.WithHangfire.Api.Models;
using BulkMessage.Orchestrator.WithHangfire.Api.Options;
using BulkMessage.Orchestrator.WithHangfire.Api.Services;
using Hangfire;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace BulkMessage.Orchestrator.WithHangfire.Api.Controllers;

[Authorize]
[ApiController]
[Route("api/message-jobs")]
public sealed class MessageJobsController(
    OrchestratorDbContext dbContext,
    IBackgroundJobClient backgroundJobClient,
    IBulkPublishProgressStore progressStore,
    ICancellationRegistry cancellationRegistry,
    IOptions<BulkPublishingOptions> options) : ControllerBase
{
    [HttpPost]
    [EnableRateLimiting("create-job")]
    public async Task<ActionResult<BulkPublishResponse>> CreateAsync(
        [FromBody] BulkPublishRequest request,
        CancellationToken cancellationToken)
    {
        var jobId = Guid.NewGuid();
        var config = options.Value;
        var batchSize = request.BatchSize.GetValueOrDefault(config.DefaultBatchSize);
        var maxParallelPublishes = request.MaxParallelPublishes.GetValueOrDefault(config.MaxParallelPublishes);
        var scheduledAt = request.ScheduleAtUtc;

        var job = new MessagePublishJob
        {
            JobId = jobId,
            MessageCount = request.MessageCount,
            BatchSize = batchSize,
            MaxParallelPublishes = maxParallelPublishes,
            PayloadTemplate = request.PayloadTemplate,
            Status = scheduledAt is null || scheduledAt <= DateTimeOffset.UtcNow ? "Queued" : "Scheduled",
            CreatedAtUtc = DateTimeOffset.UtcNow,
            ScheduledAtUtc = scheduledAt
        };

        await dbContext.MessagePublishJobs.AddAsync(job, cancellationToken);
        progressStore.Initialize(jobId, request.MessageCount);

        job.HangfireJobId = scheduledAt is not null && scheduledAt > DateTimeOffset.UtcNow
            ? backgroundJobClient.Schedule<IBulkPublishingEngine>(
                engine => engine.ExecuteAsync(jobId, CancellationToken.None),
                scheduledAt.Value - DateTimeOffset.UtcNow)
            : backgroundJobClient.Enqueue<IBulkPublishingEngine>(
                engine => engine.ExecuteAsync(jobId, CancellationToken.None));

        await dbContext.SaveChangesAsync(cancellationToken);

        return Accepted(new BulkPublishResponse(jobId, job.HangfireJobId, scheduledAt));
    }

    [HttpGet("{jobId:guid}/progress")]
    public async Task<ActionResult<BulkPublishProgress>> GetProgressAsync(Guid jobId, CancellationToken cancellationToken)
    {
        var progress = progressStore.Get(jobId);
        if (progress.TotalMessages > 0)
        {
            return Ok(progress);
        }

        var job = await dbContext.MessagePublishJobs.AsNoTracking().SingleOrDefaultAsync(x => x.JobId == jobId, cancellationToken);
        if (job is null)
        {
            return NotFound();
        }

        var response = new BulkPublishProgress(jobId, job.MessageCount, job.PublishedMessages, job.FailedMessages, job.CompletedAtUtc is not null);
        return Ok(response);
    }

    [HttpPost("{jobId:guid}/cancel")]
    public async Task<IActionResult> CancelAsync(Guid jobId, CancellationToken cancellationToken)
    {
        var job = await dbContext.MessagePublishJobs.SingleOrDefaultAsync(x => x.JobId == jobId, cancellationToken);
        if (job is null)
        {
            return NotFound();
        }

        if (job.Status is not ("Queued" or "Scheduled" or "Running"))
        {
            return Conflict(new { message = $"Job is in status '{job.Status}' and cannot be cancelled." });
        }

        cancellationRegistry.Cancel(jobId);
        return Accepted();
    }

    [HttpPost("{jobId:guid}/retry")]
    public async Task<ActionResult<BulkPublishResponse>> RetryAsync(Guid jobId, CancellationToken cancellationToken)
    {
        var job = await dbContext.MessagePublishJobs.AsNoTracking().SingleOrDefaultAsync(x => x.JobId == jobId, cancellationToken);
        if (job is null)
        {
            return NotFound();
        }

        var failedCount = await dbContext.FailedMessages.CountAsync(x => x.JobId == jobId, cancellationToken);
        if (failedCount == 0)
        {
            return Conflict(new { message = "No failed messages found for this job." });
        }

        var retryJobId = Guid.NewGuid();
        var retryJob = new MessagePublishJob
        {
            JobId = retryJobId,
            MessageCount = failedCount,
            BatchSize = job.BatchSize,
            MaxParallelPublishes = job.MaxParallelPublishes,
            PayloadTemplate = job.PayloadTemplate,
            Status = "Queued",
            CreatedAtUtc = DateTimeOffset.UtcNow
        };

        await dbContext.MessagePublishJobs.AddAsync(retryJob, cancellationToken);
        progressStore.Initialize(retryJobId, failedCount);

        retryJob.HangfireJobId = backgroundJobClient.Enqueue<IBulkPublishingEngine>(
            engine => engine.RetryFailedAsync(jobId, CancellationToken.None));

        await dbContext.SaveChangesAsync(cancellationToken);

        return Accepted(new BulkPublishResponse(retryJobId, retryJob.HangfireJobId, null));
    }
}
