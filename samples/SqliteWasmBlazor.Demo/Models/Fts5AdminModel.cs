using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using RxBlazorV2.Interface;
using RxBlazorV2.Model;
using RxBlazorV2.MudBlazor.Components;
using SqliteWasmBlazor.Models;
using SqliteWasmBlazor.Models.Extensions;

namespace SqliteWasmBlazor.Demo.Models;

/// <summary>
/// The three FTS5 index maintenance operations on the Administration page.
///
/// <para>
/// They are commands rather than click handlers because they are slow: a
/// rebuild walks every row, and on a database with real content that is long
/// enough for a screen where nothing moves to read as a screen where nothing
/// is happening. A command carries its own <c>Executing</c> state, so
/// <c>MudButtonAsyncRx</c> spins, disables itself, and offers cancellation
/// without a busy flag anywhere in the page.
/// </para>
///
/// <para>
/// The page inherits <c>EncryptionModelComponent</c> for its reset card, so
/// this model is injected rather than inherited — one model per component is
/// the rule (RXBG061), and the buttons bind the command object itself, which
/// carries its notifications with it.
/// </para>
/// </summary>
[ObservableModelScope(ModelScope.Scoped)]
[ObservableComponent]
public partial class Fts5AdminModel : ObservableModel
{
    public partial Fts5AdminModel(
        IDbContextFactory<TodoDbContext> contextFactory,
        StatusModel statusModel);

    [ObservableCommand(nameof(OptimizeAsync))]
    public partial IObservableCommandAsync Optimize { get; }

    [ObservableCommand(nameof(RebuildAsync))]
    public partial IObservableCommandAsync Rebuild { get; }

    [ObservableCommand(nameof(CheckIntegrityAsync))]
    public partial IObservableCommandAsync CheckIntegrity { get; }

    private async Task OptimizeAsync(CancellationToken cancellationToken)
    {
        var elapsed = await RunAsync(
            (context, ct) => context.OptimizeTodoItemFts5IndexAsync(ct), cancellationToken);
        StatusModel.AddSuccess(
            $"FTS5 index optimized in {elapsed}ms — index size reduced, query performance improved.",
            nameof(Optimize));
    }

    private async Task RebuildAsync(CancellationToken cancellationToken)
    {
        var elapsed = await RunAsync(
            (context, ct) => context.RebuildTodoItemFts5IndexAsync(ct), cancellationToken);
        StatusModel.AddSuccess(
            $"FTS5 index rebuilt in {elapsed}ms — full index reconstruction completed.",
            nameof(Rebuild));
    }

    private async Task CheckIntegrityAsync(CancellationToken cancellationToken)
    {
        var elapsed = await RunAsync(
            (context, ct) => context.CheckTodoItemFts5IntegrityAsync(ct), cancellationToken);
        StatusModel.AddSuccess(
            $"FTS5 integrity check passed in {elapsed}ms — no corruption detected.",
            nameof(CheckIntegrity));
    }

    /// <summary>
    /// Open a context, time the operation, dispose. A throw propagates: the
    /// command routes it to <see cref="StatusModel"/> as an error, which is
    /// what the page's snackbar calls used to do by hand.
    /// </summary>
    private async Task<long> RunAsync(
        Func<TodoDbContext, CancellationToken, ValueTask> operation,
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        await using var context = await ContextFactory.CreateDbContextAsync(cancellationToken);
        await operation(context, cancellationToken);
        stopwatch.Stop();
        return stopwatch.ElapsedMilliseconds;
    }
}