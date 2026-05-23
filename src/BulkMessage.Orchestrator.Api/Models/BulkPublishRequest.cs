using System.ComponentModel.DataAnnotations;

namespace BulkMessage.Orchestrator.Api.Models;

public sealed class BulkPublishRequest
{
    [Range(1, int.MaxValue)]
    public int MessageCount { get; init; }

    public DateTimeOffset? ScheduleAtUtc { get; init; }

    [Range(1, int.MaxValue)]
    public int? BatchSize { get; init; }

    [Range(1, 128)]
    public int? MaxParallelPublishes { get; init; }

    [Required]
    public string PayloadTemplate { get; init; } = "message-{0}";
}
