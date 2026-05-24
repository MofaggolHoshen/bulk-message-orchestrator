namespace BulkMessage.Orchestrator.WithHangfire.Api.Contracts;

public sealed record BulkMessagePublished(Guid JobId, long SequenceNumber, string Payload, DateTimeOffset ScheduledAtUtc);
