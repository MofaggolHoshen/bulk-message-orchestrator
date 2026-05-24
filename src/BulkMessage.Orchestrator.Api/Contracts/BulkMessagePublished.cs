namespace BulkMessage.Orchestrator.Api.Contracts;

public sealed record BulkMessagePublished(Guid JobId, long SequenceNumber, string Payload, DateTimeOffset ScheduledAtUtc);
