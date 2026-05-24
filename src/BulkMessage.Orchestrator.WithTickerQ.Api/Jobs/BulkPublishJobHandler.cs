using BulkMessage.Orchestrator.WithTickerQ.Api.Services;

namespace BulkMessage.Orchestrator.WithTickerQ.Api.Jobs;

public sealed class BulkPublishJobHandler(
    IBulkPublishingEngine engine,
    ILogger<BulkPublishJobHandler> logger)
{
    public async Task ExecuteAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("BulkPublishJobHandler executing for job {JobId}", jobId);
        await engine.ExecuteAsync(jobId, cancellationToken);
    }
}
