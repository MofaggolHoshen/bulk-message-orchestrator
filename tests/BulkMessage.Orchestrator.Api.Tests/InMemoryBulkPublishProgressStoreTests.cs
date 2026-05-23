using BulkMessage.Orchestrator.Api.Services;
using FluentAssertions;

namespace BulkMessage.Orchestrator.Api.Tests;

public class InMemoryBulkPublishProgressStoreTests
{
    [Fact]
    public void Update_ShouldTrackProgressAndCompletion()
    {
        var store = new InMemoryBulkPublishProgressStore();
        var jobId = Guid.NewGuid();

        store.Initialize(jobId, totalMessages: 10);
        store.Update(jobId, publishedDelta: 7, failedDelta: 2);
        var final = store.Update(jobId, publishedDelta: 1, failedDelta: 0, isCompleted: true);

        final.TotalMessages.Should().Be(10);
        final.PublishedMessages.Should().Be(8);
        final.FailedMessages.Should().Be(2);
        final.IsCompleted.Should().BeTrue();
        final.PercentageComplete.Should().Be(100m);
    }
}
