namespace BulkMessage.Orchestrator.WithHangfire.Api.Models;

public sealed record BulkPublishProgress(Guid JobId, int TotalMessages, int PublishedMessages, int FailedMessages, bool IsCompleted)
{
    public decimal PercentageComplete => TotalMessages == 0
        ? 0
        : Math.Round(((decimal)(PublishedMessages + FailedMessages) / TotalMessages) * 100, 2);
}
