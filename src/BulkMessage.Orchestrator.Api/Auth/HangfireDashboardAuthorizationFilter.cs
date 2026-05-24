using Hangfire.Dashboard;

namespace BulkMessage.Orchestrator.Api.Auth;

public sealed class HangfireDashboardAuthorizationFilter(IConfiguration configuration) : IDashboardAuthorizationFilter
{
    private const string HeaderName = "X-Api-Key";

    public bool Authorize(DashboardContext context)
    {
        var configuredKey = configuration["ApiKey"];

        // No key configured — open access (development mode)
        if (string.IsNullOrWhiteSpace(configuredKey))
        {
            return true;
        }

        var httpContext = context.GetHttpContext();
        return httpContext.Request.Headers.TryGetValue(HeaderName, out var key) && key == configuredKey;
    }
}
