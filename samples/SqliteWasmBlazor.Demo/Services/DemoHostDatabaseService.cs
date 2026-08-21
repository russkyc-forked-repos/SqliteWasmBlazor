using Microsoft.EntityFrameworkCore;
using SqliteWasmBlazor.Crypto.UI.Services;
using SqliteWasmBlazor.Models;

namespace SqliteWasmBlazor.Demo.Services;

/// <summary>
/// Demo-side <see cref="IHostDatabaseService"/> — the single place that
/// knows which databases the Demo owns and how to bring their schema up
/// to date. Two entry points:
/// <list type="bullet">
///   <item><see cref="ResetAsync"/> — "reset everything": disk back to
///   Plain, user signed out, schema re-created, boot status READY.
///   Reached from Crypto.UI's <c>DatabaseErrorAlert</c>, the encryption
///   page's Reset button, and the Administration page.</item>
///   <item><see cref="MigrateAsync"/> — the non-destructive half, run
///   after every import so the freshly landed bytes get their pending
///   migrations and an owned database the import omitted is re-created
///   before the next query hits it.</item>
/// </list>
///
/// <para>
/// Reset sequence — minimal manual orchestration; the auth signout falls
/// out of the existing reactive cascade so this service stays focused on
/// what only the host knows (its DbContexts):
/// <list type="number">
///   <item><see cref="IEncryptedSqliteWasmDatabaseService.ResetPoolAsync"/> —
///         wipes every DB file from OPFS, drops the worker globalKey,
///         clears the PRF cache. The cache clear emits
///         <c>IPrfService.KeyExpired</c>, which
///         <c>AuthenticationModel.OnSessionExpiredAsync</c> consumes;
///         the handler reads the now-empty manifest and full-signs-out
///         (PublicKey=null + CredentialId=null) without manual wiring
///         here.</item>
///   <item><see cref="MigrateAsync"/> — re-creates the schema on the
///         now-empty Plain disk and reports
///         <see cref="DbInitState.READY"/>.</item>
/// </list>
/// </para>
///
/// <para>
/// Adding a new DbContext to the Demo: inject the new factory, add one
/// <c>MigrateAsync</c> call, and add its file name to
/// <see cref="OwnedDatabases"/>. Every Reset, import and panel row picks
/// it up automatically — no per-callsite enumeration.
/// </para>
/// </summary>
public sealed class DemoHostDatabaseService : IHostDatabaseService
{
    private readonly IEncryptedSqliteWasmDatabaseService _session;
    private readonly IDbContextFactory<TodoDbContext> _todoFactory;
    private readonly IDbContextFactory<NoteDbContext> _noteFactory;
    private readonly IDbInitializationReporter _reporter;

    public DemoHostDatabaseService(
        IEncryptedSqliteWasmDatabaseService session,
        IDbContextFactory<TodoDbContext> todoFactory,
        IDbContextFactory<NoteDbContext> noteFactory,
        IDbInitializationReporter reporter)
    {
        _session = session;
        _todoFactory = todoFactory;
        _noteFactory = noteFactory;
        _reporter = reporter;
    }

    public bool IsAvailable => true;

    /// <summary>
    /// The two databases <c>Program.cs</c> wires a <c>DbContext</c> to.
    /// Anything else in the pool is storage the Demo doesn't read.
    /// </summary>
    public IReadOnlyList<string> OwnedDatabases { get; } = ["TodoDb.db", "NotesDb.db"];

    public async ValueTask ResetAsync(CancellationToken cancellationToken = default)
    {
        // Scorched-earth disk wipe. PRF cache clear cascades through
        // KeyExpired → AuthenticationModel.OnSessionExpiredAsync, which
        // reads the now-empty manifest and full-signs-out (clears both
        // PublicKey AND CredentialId). No manual Auth.SignOut here.
        await _session.ResetPoolAsync(cancellationToken);

        // Re-create the schema on the now-empty pool.
        await MigrateAsync(cancellationToken);
    }

    public async ValueTask MigrateAsync(CancellationToken cancellationToken = default)
    {
        // Re-migrate every consumer DbContext. The host-specific step:
        // MigrateAsync creates the database when it is missing and applies
        // pending migrations when an import brought in an older schema.
        await using (var todoCtx = await _todoFactory.CreateDbContextAsync(cancellationToken))
        {
            await todoCtx.Database.MigrateAsync(cancellationToken);
        }

        await using (var noteCtx = await _noteFactory.CreateDbContextAsync(cancellationToken))
        {
            await noteCtx.Database.MigrateAsync(cancellationToken);
        }

        // Clear any lingering boot-failure alert.
        _reporter.Report(DbInitState.READY);
    }
}
