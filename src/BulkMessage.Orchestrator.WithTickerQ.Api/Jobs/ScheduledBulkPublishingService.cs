using BulkMessage.Orchestrator.WithTickerQ.Api.Data;
using BulkMessage.Orchestrator.WithTickerQ.Api.Entities;
using BulkMessage.Orchestrator.WithTickerQ.Api.Options;
using BulkMessage.Orchestrator.WithTickerQ.Api.Services;
using Microsoft.Extensions.Options;

namespace BulkMessage.Orchestrator.WithTickerQ.Api.Jobs;

public sealed class ScheduledBulkPublishingService(
    IServiceProvider serviceProvider,
    IOptions<BulkPublishingOptions> defaultOptions,
    ILogger<ScheduledBulkPublishingService> logger) : BackgroundService
{
    private readonly List<RecurringScheduleOptions> _schedules = [];
    private readonly Dictionary<string, DateTimeOffset?> _lastExecutionTimes = [];

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        // Load schedules from configuration
        var config = serviceProvider.GetRequiredService<IConfiguration>();
        var schedules = config.GetSection("RecurringSchedules").Get<List<RecurringScheduleOptions>>() ?? [];
        _schedules.AddRange(schedules.Where(s => s.Enabled && !string.IsNullOrWhiteSpace(s.Id) && !string.IsNullOrWhiteSpace(s.Cron)));

        if (_schedules.Count == 0)
        {
            logger.LogInformation("No recurring schedules configured");
            return;
        }

        logger.LogInformation("Loaded {Count} recurring schedules", _schedules.Count);

        // TODO: Implement proper cron evaluation with a cron parser library (e.g., CronExpressionDescriptor)
        // For now, we'll do a simple polling approach

        using var timer = new PeriodicTimer(TimeSpan.FromMinutes(1));

        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                await EvaluateSchedulesAsync(cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation("Scheduled publishing service stopped");
        }
    }

    private async Task EvaluateSchedulesAsync(CancellationToken cancellationToken)
    {
        using var scope = serviceProvider.CreateScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<OrchestratorDbContext>();
        var progressStore = scope.ServiceProvider.GetRequiredService<IBulkPublishProgressStore>();

        foreach (var schedule in _schedules)
        {
            // TODO: Implement proper cron evaluation
            // For now, skip scheduled job evaluation (can be extended with CronExpressionDescriptor)
            logger.LogDebug("Schedule {ScheduleId} checked (cron evaluation not yet implemented)", schedule.Id);
        }
    }
}
