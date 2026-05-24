using BulkMessage.Orchestrator.Api.Auth;
using BulkMessage.Orchestrator.Api.Data;
using BulkMessage.Orchestrator.Api.Hubs;
using BulkMessage.Orchestrator.Api.Jobs;
using BulkMessage.Orchestrator.Api.Options;
using BulkMessage.Orchestrator.Api.Services;
using Hangfire;
using Hangfire.MemoryStorage;
using MassTransit;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.EntityFrameworkCore;
using System.Threading.RateLimiting;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<BulkPublishingOptions>(builder.Configuration.GetSection("BulkPublishing"));
var recurringSchedules = builder.Configuration.GetSection("RecurringSchedules").Get<List<RecurringScheduleOptions>>() ?? [];

var sqlConnection = builder.Configuration.GetConnectionString("SqlServer");
if (string.IsNullOrWhiteSpace(sqlConnection))
{
    builder.Services.AddDbContext<OrchestratorDbContext>(options => options.UseInMemoryDatabase("orchestrator-db"));
}
else
{
    builder.Services.AddDbContext<OrchestratorDbContext>(options => options.UseSqlServer(sqlConnection));
}

builder.Services.AddHangfire(configuration =>
{
    var hangfireConnection = builder.Configuration.GetConnectionString("Hangfire");
    if (string.IsNullOrWhiteSpace(hangfireConnection))
    {
        configuration.UseMemoryStorage();
    }
    else
    {
        configuration.UseSqlServerStorage(hangfireConnection);
    }
});
builder.Services.AddHangfireServer();

builder.Services.AddMassTransit(bus =>
{
    var serviceBusConnection = builder.Configuration.GetConnectionString("AzureServiceBus");
    if (string.IsNullOrWhiteSpace(serviceBusConnection))
    {
        bus.UsingInMemory((context, cfg) =>
        {
            cfg.UseMessageRetry(retry => retry.Interval(3, TimeSpan.FromSeconds(2)));
            cfg.UseInMemoryOutbox(context);
        });
    }
    else
    {
        bus.UsingAzureServiceBus((context, cfg) =>
        {
            cfg.Host(serviceBusConnection);
            cfg.UseMessageRetry(retry => retry.Interval(3, TimeSpan.FromSeconds(2)));
            cfg.UseInMemoryOutbox(context);
        });
    }
});

// Authentication — API key scheme (open in dev when ApiKey config is empty)
builder.Services
    .AddAuthentication(ApiKeyAuthenticationHandler.Scheme)
    .AddScheme<AuthenticationSchemeOptions, ApiKeyAuthenticationHandler>(ApiKeyAuthenticationHandler.Scheme, _ => { });
builder.Services.AddAuthorization();

// Rate limiting — fixed window: 60 requests/minute on the create endpoint
builder.Services.AddRateLimiter(limiter =>
{
    limiter.AddFixedWindowLimiter("create-job", opts =>
    {
        opts.PermitLimit = 60;
        opts.Window = TimeSpan.FromMinutes(1);
        opts.QueueProcessingOrder = QueueProcessingOrder.OldestFirst;
        opts.QueueLimit = 0;
    });
    limiter.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
});

builder.Services.AddHealthChecks().AddDbContextCheck<OrchestratorDbContext>();
builder.Services.AddSignalR();
builder.Services.AddOpenApi();
builder.Services.AddControllers();

builder.Services.AddSingleton<IBulkPublishProgressStore, InMemoryBulkPublishProgressStore>();
builder.Services.AddSingleton<ICancellationRegistry, InMemoryCancellationRegistry>();
builder.Services.AddScoped<IBulkPublishingEngine, BulkPublishingEngine>();
builder.Services.AddScoped<ScheduledBulkPublishingJob>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.MapHealthChecks("/health");
app.UseAuthentication();
app.UseAuthorization();
app.UseRateLimiter();
app.UseHangfireDashboard("/hangfire", new Hangfire.DashboardOptions
{
    Authorization = [new HangfireDashboardAuthorizationFilter(app.Configuration)]
});
app.MapControllers();
app.MapHub<ProgressHub>("/hubs/progress");

using (var scope = app.Services.CreateScope())
{
    var recurringManager = scope.ServiceProvider.GetRequiredService<IRecurringJobManager>();

    foreach (var schedule in recurringSchedules.Where(x => x.Enabled && !string.IsNullOrWhiteSpace(x.Id) && !string.IsNullOrWhiteSpace(x.Cron)))
    {
        var captured = schedule;
        recurringManager.AddOrUpdate<ScheduledBulkPublishingJob>(
            schedule.Id,
            job => job.ExecuteAsync(captured),
            schedule.Cron,
            new RecurringJobOptions
            {
                TimeZone = TimeZoneInfo.Utc
            });
    }
}

app.Run();

public partial class Program;
