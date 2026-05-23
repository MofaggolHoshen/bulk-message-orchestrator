namespace BulkMessage.Orchestrator.Api.Options;

public sealed class RecurringScheduleOptions
{
    public string Id { get; set; } = string.Empty;

    public string Cron { get; set; } = string.Empty;

    public bool Enabled { get; set; } = true;
}
