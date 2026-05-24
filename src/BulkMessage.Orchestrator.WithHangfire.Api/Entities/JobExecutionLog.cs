namespace BulkMessage.Orchestrator.WithHangfire.Api.Entities;

public sealed class JobExecutionLog
{
    public long Id { get; set; }

    public Guid JobId { get; set; }

    public string Level { get; set; } = "Information";

    public string Message { get; set; } = string.Empty;

    public DateTimeOffset CreatedAtUtc { get; set; }
}
