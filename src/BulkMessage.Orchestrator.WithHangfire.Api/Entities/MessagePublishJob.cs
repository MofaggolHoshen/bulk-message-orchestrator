namespace BulkMessage.Orchestrator.WithHangfire.Api.Entities;

public sealed class MessagePublishJob
{
    public Guid JobId { get; set; }

    public int MessageCount { get; set; }

    public int BatchSize { get; set; }

    public int MaxParallelPublishes { get; set; }

    public string PayloadTemplate { get; set; } = string.Empty;

    public string Status { get; set; } = "Queued";

    public string? HangfireJobId { get; set; }

    public DateTimeOffset CreatedAtUtc { get; set; }

    public DateTimeOffset? ScheduledAtUtc { get; set; }

    public DateTimeOffset? StartedAtUtc { get; set; }

    public DateTimeOffset? CompletedAtUtc { get; set; }

    public int PublishedMessages { get; set; }

    public int FailedMessages { get; set; }

    public string? LastError { get; set; }
}
