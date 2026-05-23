using Microsoft.AspNetCore.SignalR;

namespace BulkMessage.Orchestrator.Api.Hubs;

public sealed class ProgressHub : Hub
{
    public Task SubscribeToJob(Guid jobId)
    {
        return Groups.AddToGroupAsync(Context.ConnectionId, jobId.ToString());
    }
}
