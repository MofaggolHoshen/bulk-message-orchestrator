using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;
using System.Security.Claims;
using System.Text.Encodings.Web;

namespace BulkMessage.Orchestrator.Api.Auth;

public sealed class ApiKeyAuthenticationHandler(
    IOptionsMonitor<AuthenticationSchemeOptions> options,
    ILoggerFactory logger,
    UrlEncoder encoder,
    IConfiguration configuration)
    : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
{
    public new const string Scheme = "ApiKey";
    private const string HeaderName = "X-Api-Key";

    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        var configuredKey = configuration["ApiKey"];

        // No key configured — open access (development mode)
        if (string.IsNullOrWhiteSpace(configuredKey))
        {
            return Task.FromResult(AuthenticateResult.Success(BuildTicket("anonymous")));
        }

        if (!Request.Headers.TryGetValue(HeaderName, out var providedKey) || providedKey != configuredKey)
        {
            return Task.FromResult(AuthenticateResult.Fail("Invalid or missing X-Api-Key header."));
        }

        return Task.FromResult(AuthenticateResult.Success(BuildTicket("api-client")));
    }

    private AuthenticationTicket BuildTicket(string name)
    {
        var claims = new[] { new Claim(ClaimTypes.Name, name) };
        var identity = new ClaimsIdentity(claims, Scheme);
        var principal = new ClaimsPrincipal(identity);
        return new AuthenticationTicket(principal, Scheme);
    }
}
