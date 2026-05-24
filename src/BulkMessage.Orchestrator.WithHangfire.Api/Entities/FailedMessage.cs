namespace BulkMessage.Orchestrator.WithHangfire.Api.Entities;

public sealed class FailedMessage
{
    public long Id { get; set; }

    public Guid JobId { get; set; }

    public long SequenceNumber { get; set; }

    public string Payload { get; set; } = string.Empty;

    public string Error { get; set; } = string.Empty;

    public DateTimeOffset FailedAtUtc { get; set; }
}
