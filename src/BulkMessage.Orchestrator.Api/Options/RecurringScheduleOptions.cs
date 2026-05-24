namespace BulkMessage.Orchestrator.Api.Options;

public sealed class RecurringScheduleOptions
{
    public string Id { get; set; } = string.Empty;

    public string Cron { get; set; } = string.Empty;

    public bool Enabled { get; set; } = true;

    public int MessageCount { get; set; } = 1000;

    public int? BatchSize { get; set; }

    public int? MaxParallelPublishes { get; set; }

    public string PayloadTemplate { get; set; } = "scheduled-message-{0}";
}
