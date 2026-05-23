using BulkMessage.Orchestrator.Api.Services;

namespace BulkMessage.Orchestrator.Api.Jobs;

public sealed class ScheduledBulkPublishingJob(ILogger<ScheduledBulkPublishingJob> logger)
{
    public Task ExecuteAsync()
    {
        logger.LogInformation("Recurring schedule executed at {TimestampUtc}", DateTimeOffset.UtcNow);
        return Task.CompletedTask;
    }
}
