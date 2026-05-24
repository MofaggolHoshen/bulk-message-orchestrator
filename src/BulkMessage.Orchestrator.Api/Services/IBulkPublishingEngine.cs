namespace BulkMessage.Orchestrator.Api.Services;

public interface IBulkPublishingEngine
{
    Task ExecuteAsync(Guid jobId, CancellationToken cancellationToken = default);

    Task RetryFailedAsync(Guid sourceJobId, CancellationToken cancellationToken = default);
}
