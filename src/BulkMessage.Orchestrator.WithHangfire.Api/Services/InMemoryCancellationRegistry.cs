using System.Collections.Concurrent;

namespace BulkMessage.Orchestrator.WithHangfire.Api.Services;

public sealed class InMemoryCancellationRegistry : ICancellationRegistry
{
    private readonly ConcurrentDictionary<Guid, CancellationTokenSource> _sources = new();

    public CancellationToken Register(Guid jobId)
    {
        var cts = _sources.GetOrAdd(jobId, _ => new CancellationTokenSource());
        return cts.Token;
    }

    public bool Cancel(Guid jobId)
    {
        if (_sources.TryGetValue(jobId, out var cts))
        {
            cts.Cancel();
            return true;
        }
        return false;
    }

    public void Unregister(Guid jobId)
    {
        if (_sources.TryRemove(jobId, out var cts))
        {
            cts.Dispose();
        }
    }
}
