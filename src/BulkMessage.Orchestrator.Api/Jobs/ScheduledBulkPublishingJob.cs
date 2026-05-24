using BulkMessage.Orchestrator.Api.Data;
using BulkMessage.Orchestrator.Api.Entities;
using BulkMessage.Orchestrator.Api.Options;
using BulkMessage.Orchestrator.Api.Services;
using Hangfire;
using Microsoft.Extensions.Options;

namespace BulkMessage.Orchestrator.Api.Jobs;

public sealed class ScheduledBulkPublishingJob(
    OrchestratorDbContext dbContext,
    IBackgroundJobClient backgroundJobClient,
    IBulkPublishProgressStore progressStore,
    IOptions<BulkPublishingOptions> defaultOptions,
    ILogger<ScheduledBulkPublishingJob> logger)
{
    public async Task ExecuteAsync(RecurringScheduleOptions schedule)
    {
        var config = defaultOptions.Value;
        var jobId = Guid.NewGuid();
        var batchSize = schedule.BatchSize ?? config.DefaultBatchSize;
        var maxParallel = schedule.MaxParallelPublishes ?? config.MaxParallelPublishes;

        logger.LogInformation(
            "Recurring schedule {ScheduleId} triggered — creating job {JobId} for {MessageCount} messages",
            schedule.Id, jobId, schedule.MessageCount);

        var job = new MessagePublishJob
        {
            JobId = jobId,
            MessageCount = schedule.MessageCount,
            BatchSize = batchSize,
            MaxParallelPublishes = maxParallel,
            PayloadTemplate = schedule.PayloadTemplate,
            Status = "Queued",
            CreatedAtUtc = DateTimeOffset.UtcNow
        };

        await dbContext.MessagePublishJobs.AddAsync(job);
        progressStore.Initialize(jobId, schedule.MessageCount);

        job.HangfireJobId = backgroundJobClient.Enqueue<IBulkPublishingEngine>(
            engine => engine.ExecuteAsync(jobId, CancellationToken.None));

        await dbContext.SaveChangesAsync();

        logger.LogInformation("Job {JobId} enqueued with Hangfire job ID {HangfireJobId}", jobId, job.HangfireJobId);
    }
}
