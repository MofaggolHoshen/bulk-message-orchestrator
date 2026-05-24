using BulkMessage.Orchestrator.WithTickerQ.Api.Services;

namespace BulkMessage.Orchestrator.WithTickerQ.Api.Jobs;

public sealed class RetryFailedJobHandler(
    IBulkPublishingEngine engine,
    ILogger<RetryFailedJobHandler> logger)
{
    public async Task ExecuteAsync(Guid sourceJobId, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("RetryFailedJobHandler executing for source job {JobId}", sourceJobId);
        await engine.RetryFailedAsync(sourceJobId, cancellationToken);
    }
}
