using BulkMessage.Orchestrator.Api.Data;
using BulkMessage.Orchestrator.Api.Entities;
using BulkMessage.Orchestrator.Api.Models;
using BulkMessage.Orchestrator.Api.Options;
using BulkMessage.Orchestrator.Api.Services;
using Hangfire;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace BulkMessage.Orchestrator.Api.Controllers;

[ApiController]
[Route("api/message-jobs")]
public sealed class MessageJobsController(
    OrchestratorDbContext dbContext,
    IBackgroundJobClient backgroundJobClient,
    IBulkPublishProgressStore progressStore,
    IOptions<BulkPublishingOptions> options) : ControllerBase
{
    [HttpPost]
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
}
