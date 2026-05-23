using BulkMessage.Orchestrator.Api.Models;

namespace BulkMessage.Orchestrator.Api.Services;

public interface IBulkPublishProgressStore
{
    void Initialize(Guid jobId, int totalMessages);

    BulkPublishProgress Get(Guid jobId);

    BulkPublishProgress Update(Guid jobId, int publishedDelta, int failedDelta, bool isCompleted = false);
}
