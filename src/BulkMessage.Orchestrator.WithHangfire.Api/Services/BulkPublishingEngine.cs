using BulkMessage.Orchestrator.Api.Contracts;
using BulkMessage.Orchestrator.Api.Models;
using BulkMessage.Orchestrator.WithHangfire.Api.Data;
using BulkMessage.Orchestrator.WithHangfire.Api.Entities;
using BulkMessage.Orchestrator.WithHangfire.Api.Hubs;
using MassTransit;
using Microsoft.AspNetCore.SignalR;
using Microsoft.EntityFrameworkCore;

namespace BulkMessage.Orchestrator.WithHangfire.Api.Services;

public sealed class BulkPublishingEngine(
    OrchestratorDbContext dbContext,
    IPublishEndpoint publishEndpoint,
    IBulkPublishProgressStore progressStore,
    ICancellationRegistry cancellationRegistry,
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

        // Combine the Hangfire-provided token with the registry token so either side can cancel.
        using var registryToken = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken, cancellationRegistry.Register(jobId));

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

        try
        {
            foreach (var chunk in Enumerable.Range(1, job.MessageCount).Chunk(job.BatchSize))
            {
                registryToken.Token.ThrowIfCancellationRequested();

                var chunkPublished = 0;
                var chunkFailed = 0;
                var failedMessages = new List<FailedMessage>();

                await Parallel.ForEachAsync(
                    chunk,
                    new ParallelOptions
                    {
                        MaxDegreeOfParallelism = job.MaxParallelPublishes,
                        CancellationToken = registryToken.Token
                    },
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
                        catch (OperationCanceledException)
                        {
                            throw;
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

            await dbContext.JobExecutionLogs.AddAsync(new JobExecutionLog
            {
                JobId = jobId,
                CreatedAtUtc = DateTimeOffset.UtcNow,
                Level = totalFailed == 0 ? "Information" : "Warning",
                Message = totalFailed == 0
                    ? "Job completed successfully"
                    : $"Job completed with {totalFailed} failed messages"
            }, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Job {JobId} was cancelled", jobId);
            job.Status = "Cancelled";
            job.CompletedAtUtc = DateTimeOffset.UtcNow;
            job.LastError = "Job was cancelled by request.";

            await dbContext.JobExecutionLogs.AddAsync(new JobExecutionLog
            {
                JobId = jobId,
                CreatedAtUtc = DateTimeOffset.UtcNow,
                Level = "Warning",
                Message = "Job was cancelled"
            }, CancellationToken.None);
        }
        finally
        {
            cancellationRegistry.Unregister(jobId);
        }

        await dbContext.SaveChangesAsync(CancellationToken.None);

        var finalProgress = progressStore.Update(jobId, 0, 0, isCompleted: true);
        await progressHub.Clients.Group(jobId.ToString()).SendAsync("progress-updated", finalProgress, CancellationToken.None);
    }

    public async Task RetryFailedAsync(Guid sourceJobId, CancellationToken cancellationToken = default)
    {
        var failedMessages = await dbContext.FailedMessages
            .Where(x => x.JobId == sourceJobId)
            .ToListAsync(cancellationToken);

        if (failedMessages.Count == 0)
        {
            logger.LogInformation("No failed messages found for job {JobId}", sourceJobId);
            return;
        }

        logger.LogInformation("Retrying {Count} failed messages for job {JobId}", failedMessages.Count, sourceJobId);

        var retried = new List<FailedMessage>();
        var stillFailed = new List<FailedMessage>();

        await Parallel.ForEachAsync(
            failedMessages,
            new ParallelOptions { MaxDegreeOfParallelism = 8, CancellationToken = cancellationToken },
            async (failed, ct) =>
            {
                try
                {
                    await publishEndpoint.Publish(
                        new BulkMessagePublished(sourceJobId, failed.SequenceNumber, failed.Payload, DateTimeOffset.UtcNow),
                        ct);
                    lock (retried) { retried.Add(failed); }
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Retry failed for sequence {Seq} in job {JobId}", failed.SequenceNumber, sourceJobId);
                    lock (stillFailed) { stillFailed.Add(failed); }
                }
            });

        // Remove successfully retried entries
        if (retried.Count > 0)
        {
            dbContext.FailedMessages.RemoveRange(retried);
        }

        await dbContext.JobExecutionLogs.AddAsync(new JobExecutionLog
        {
            JobId = sourceJobId,
            CreatedAtUtc = DateTimeOffset.UtcNow,
            Level = stillFailed.Count == 0 ? "Information" : "Warning",
            Message = stillFailed.Count == 0
                ? $"Retry succeeded: {retried.Count} messages re-published"
                : $"Retry partial: {retried.Count} succeeded, {stillFailed.Count} still failing"
        }, cancellationToken);

        await dbContext.SaveChangesAsync(cancellationToken);
    }
}
