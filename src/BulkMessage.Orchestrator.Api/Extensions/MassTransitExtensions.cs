using MassTransit;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace BulkMessage.Orchestrator.Api.Extensions;

public static class MassTransitExtensions
{
    public static IServiceCollection AddMassTransitConfiguration(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.AddMassTransit(bus =>
        {
            var serviceBusConnection = configuration.GetConnectionString("AzureServiceBus");
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

        return services;
    }
}
