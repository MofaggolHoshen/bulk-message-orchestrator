namespace BulkMessage.Orchestrator.Api.Models;

public sealed record BulkPublishResponse(Guid JobId, string HangfireJobId, DateTimeOffset? ScheduledAtUtc);
