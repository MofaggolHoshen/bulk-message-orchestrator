namespace BulkMessage.Orchestrator.WithTickerQ.Api.Models;

public sealed record BulkPublishResponse(Guid JobId, string TickerQJobId, DateTimeOffset? ScheduledAtUtc);

