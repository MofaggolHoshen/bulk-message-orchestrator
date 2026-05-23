namespace BulkMessage.Orchestrator.Api.Options;

public sealed class BulkPublishingOptions
{
    public int DefaultBatchSize { get; set; } = 500;

    public int MaxParallelPublishes { get; set; } = 8;

    public int RetryCount { get; set; } = 3;
}
