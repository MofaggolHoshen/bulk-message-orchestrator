using BulkMessage.Orchestrator.Api.Entities;
using Microsoft.EntityFrameworkCore;

namespace BulkMessage.Orchestrator.Api.Data;

public sealed class OrchestratorDbContext(DbContextOptions<OrchestratorDbContext> options) : DbContext(options)
{
    public DbSet<MessagePublishJob> MessagePublishJobs => Set<MessagePublishJob>();

    public DbSet<FailedMessage> FailedMessages => Set<FailedMessage>();

    public DbSet<JobExecutionLog> JobExecutionLogs => Set<JobExecutionLog>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MessagePublishJob>(entity =>
        {
            entity.HasKey(x => x.JobId);
            entity.Property(x => x.PayloadTemplate).HasMaxLength(4000);
            entity.Property(x => x.Status).HasMaxLength(64);
            entity.Property(x => x.HangfireJobId).HasMaxLength(128);
            entity.Property(x => x.LastError).HasMaxLength(2048);
        });

        modelBuilder.Entity<FailedMessage>(entity =>
        {
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Payload).HasMaxLength(4000);
            entity.Property(x => x.Error).HasMaxLength(2048);
            entity.HasIndex(x => new { x.JobId, x.SequenceNumber });
        });

        modelBuilder.Entity<JobExecutionLog>(entity =>
        {
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Level).HasMaxLength(32);
            entity.Property(x => x.Message).HasMaxLength(2048);
            entity.HasIndex(x => x.JobId);
        });
    }
}
