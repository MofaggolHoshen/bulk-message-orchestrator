using BulkMessage.Orchestrator.Api.Contracts;
using BulkMessage.Orchestrator.Api.Data;
using BulkMessage.Orchestrator.Api.Entities;
using BulkMessage.Orchestrator.Api.Hubs;
using BulkMessage.Orchestrator.Api.Models;
using MassTransit;
using Microsoft.AspNetCore.SignalR;
using Microsoft.EntityFrameworkCore;

namespace BulkMessage.Orchestrator.Api.Services;

public sealed class BulkPublishingEngine(
    OrchestratorDbContext dbContext,
    IPublishEndpoint publishEndpoint,
    IBulkPublishProgressStore progressStore,
    IHubContext<ProgressHub> progressHub,
    ILogger<BulkPublishingEngine> logger) : IBulkPublishingEngine
{
    public async Task ExecuteAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        var job = await dbContext.MessagePublishJobs.SingleOrDefaultAsync(x => x.JobId == jobId, cancellationToken);
        if (job is null)
        {
            logger.LogWarning("Message publish job {JobId} not found", jobId);
            return;
        }

        job.Status = "Running";
        job.StartedAtUtc = DateTimeOffset.UtcNow;
        await dbContext.JobExecutionLogs.AddAsync(new JobExecutionLog
        {
            JobId = jobId,
            CreatedAtUtc = DateTimeOffset.UtcNow,
            Level = "Information",
            Message = "Job started"
        }, cancellationToken);
        await dbContext.SaveChangesAsync(cancellationToken);

        progressStore.Initialize(jobId, job.MessageCount);

        var totalPublished = 0;
        var totalFailed = 0;

        foreach (var chunk in Enumerable.Range(1, job.MessageCount).Chunk(job.BatchSize))
        {
            var chunkPublished = 0;
            var chunkFailed = 0;
            var failedMessages = new List<FailedMessage>();

            await Parallel.ForEachAsync(
                chunk,
                new ParallelOptions { MaxDegreeOfParallelism = job.MaxParallelPublishes, CancellationToken = cancellationToken },
                async (sequence, ct) =>
                {
                    var payload = string.Format(job.PayloadTemplate, sequence);
                    try
                    {
                        await publishEndpoint.Publish(
                            new BulkMessagePublished(jobId, sequence, payload, DateTimeOffset.UtcNow),
                            ct);
                        Interlocked.Increment(ref chunkPublished);
                    }
                    catch (Exception ex)
                    {
                        Interlocked.Increment(ref chunkFailed);
                        lock (failedMessages)
                        {
                            failedMessages.Add(new FailedMessage
                            {
                                JobId = jobId,
                                SequenceNumber = sequence,
                                Payload = payload,
                                Error = ex.Message,
                                FailedAtUtc = DateTimeOffset.UtcNow
                            });
                        }
                    }
                });

            if (failedMessages.Count > 0)
            {
                await dbContext.FailedMessages.AddRangeAsync(failedMessages, cancellationToken);
            }

            totalPublished += chunkPublished;
            totalFailed += chunkFailed;
            job.PublishedMessages = totalPublished;
            job.FailedMessages = totalFailed;

            var progress = progressStore.Update(jobId, chunkPublished, chunkFailed);
            await progressHub.Clients.Group(jobId.ToString()).SendAsync("progress-updated", progress, cancellationToken);

            await dbContext.SaveChangesAsync(cancellationToken);
        }

        job.Status = totalFailed == 0 ? "Completed" : "CompletedWithFailures";
        job.CompletedAtUtc = DateTimeOffset.UtcNow;
        var finalProgress = progressStore.Update(jobId, 0, 0, isCompleted: true);

        await dbContext.JobExecutionLogs.AddAsync(new JobExecutionLog
        {
            JobId = jobId,
            CreatedAtUtc = DateTimeOffset.UtcNow,
            Level = totalFailed == 0 ? "Information" : "Warning",
            Message = totalFailed == 0
                ? "Job completed successfully"
                : $"Job completed with {totalFailed} failed messages"
        }, cancellationToken);

        await dbContext.SaveChangesAsync(cancellationToken);
        await progressHub.Clients.Group(jobId.ToString()).SendAsync("progress-updated", finalProgress, cancellationToken);
    }
}
