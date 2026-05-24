namespace BulkMessage.Orchestrator.WithHangfire.Api.Models;

public sealed record BulkPublishResponse(Guid JobId, string HangfireJobId, DateTimeOffset? ScheduledAtUtc);
