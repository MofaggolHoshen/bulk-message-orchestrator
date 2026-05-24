using BulkMessage.Orchestrator.Api.Models;
using FluentAssertions;

namespace BulkMessage.Orchestrator.WithHangfire.Api.Tests;

public class BulkPublishProgressTests
{
    [Fact]
    public void PercentageComplete_ShouldIncludePublishedAndFailedMessages()
    {
        var progress = new BulkPublishProgress(Guid.NewGuid(), 1000, 750, 50, false);

        progress.PercentageComplete.Should().Be(80m);
    }
}
