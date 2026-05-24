using System.Collections.Concurrent;
using BulkMessage.Orchestrator.WithHangfire.Api.Models;

namespace BulkMessage.Orchestrator.WithHangfire.Api.Services;

public sealed class InMemoryBulkPublishProgressStore : IBulkPublishProgressStore
{
    private readonly ConcurrentDictionary<Guid, BulkPublishProgress> _progress = new();

    public void Initialize(Guid jobId, int totalMessages)
    {
        _progress[jobId] = new BulkPublishProgress(jobId, totalMessages, 0, 0, false);
    }

    public BulkPublishProgress Get(Guid jobId)
    {
        return _progress.TryGetValue(jobId, out var progress)
            ? progress
            : new BulkPublishProgress(jobId, 0, 0, 0, false);
    }

    public BulkPublishProgress Update(Guid jobId, int publishedDelta, int failedDelta, bool isCompleted = false)
    {
        return _progress.AddOrUpdate(
            jobId,
            id => new BulkPublishProgress(id, publishedDelta + failedDelta, publishedDelta, failedDelta, isCompleted),
            (_, current) => current with
            {
                PublishedMessages = current.PublishedMessages + publishedDelta,
                FailedMessages = current.FailedMessages + failedDelta,
                IsCompleted = isCompleted || current.IsCompleted
            });
    }
}
