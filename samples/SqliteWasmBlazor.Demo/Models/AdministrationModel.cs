using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using RxBlazorV2.Interface;
using RxBlazorV2.Model;
using RxBlazorV2.MudBlazor.Components;
using SqliteWasmBlazor.Crypto.UI.Components.Encryption;
using SqliteWasmBlazor.Models;
using SqliteWasmBlazor.Models.Extensions;

namespace SqliteWasmBlazor.Demo.Models;

/// <summary>
/// The Administration page's model: FTS5 index maintenance of its own, plus
/// the encryption panel's reset card reached through a model reference.
///
/// <para>
/// The FTS5 operations are commands rather than click handlers because they
/// are slow: a rebuild walks every row, and on a database with real content
/// that is long enough for a screen where nothing moves to read as a screen
/// where nothing is happening. A command carries its own <c>Executing</c> state, so
/// <c>MudButtonAsyncRx</c> spins, disables itself, and offers cancellation
/// without a busy flag anywhere in the page.
/// </para>
///
/// <para>
/// <b>Why the reference.</b> A component is reactive for exactly one model,
/// and it is the component's re-render that makes a bound
/// <c>MudButtonAsyncRx</c> show its command's <c>Executing</c> state — the
/// button reads that at render time rather than subscribing itself. So a
/// command reached through a second, merely injected model never animates.
/// Models compose through the partial constructor instead: the page binds
/// this model, this model references <see cref="EncryptionModel"/>, and both
/// sets of buttons live under one reactive component. RXBG061 forbids
/// composing two <c>*ModelComponent</c> panels in one assembly; it says
/// nothing about a model referencing another model, and RXBG052 restricts
/// only component-trigger hook generation across assemblies, not the
/// reference itself.
/// </para>
/// </summary>
[ObservableModelScope(ModelScope.Scoped)]
[ObservableComponent]
public partial class AdministrationModel : ObservableModel
{
    public partial AdministrationModel(
        IDbContextFactory<TodoDbContext> contextFactory,
        StatusModel statusModel,
        EncryptionModel encryption);

    // Encryption is reached by path from the markup (Model.Encryption.Reset,
    // .ResetLabel, .ResetHint, .ResetConfirmation, .Localizer). That is not a
    // style choice: the generator merges the referenced model's stream into
    // this one re-prefixed as "Model.Encryption.X", and the component's
    // Filter() is built from the names the markup uses. A pass-through
    // property here would put "Model.ResetPool" in that filter — a name
    // nothing ever emits — and the reset card would stop re-rendering.

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