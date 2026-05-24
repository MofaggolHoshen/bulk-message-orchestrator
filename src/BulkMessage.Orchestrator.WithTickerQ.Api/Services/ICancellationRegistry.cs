namespace BulkMessage.Orchestrator.WithTickerQ.Api.Services;

public interface ICancellationRegistry
{
    /// <summary>Registers a cancellable token for the given job. Returns the token to observe.</summary>
    CancellationToken Register(Guid jobId);

    /// <summary>Signals cancellation for the given job. Returns false if no registration was found.</summary>
    bool Cancel(Guid jobId);

    /// <summary>Removes and disposes the registration after a job completes.</summary>
    void Unregister(Guid jobId);
}
