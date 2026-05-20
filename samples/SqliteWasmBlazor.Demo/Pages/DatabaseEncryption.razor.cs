using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Forms;
using MudBlazor;
using SqliteWasmBlazor.Components.Interop;
using SqliteWasmBlazor.Crypto.UI.Components.Encryption;

namespace SqliteWasmBlazor.Demo.Pages;

public partial class DatabaseEncryption
{
    // .eds goes through the streaming BlobSession path (no managed byte[]
    // of the envelope is ever allocated). .zip still loads into a managed
    // byte[] via ReadPickedAsync — use file.Size as the cap there; the
    // browser/picker already bounds what the user can hand in, and the
    // arbitrary 100 MiB ceiling rejected legitimate large plain ZIPs.
    // Future work (G8.7): flip .zip to a JS-side ZIP parser + IBrowserFile
    // streaming so the .zip cap can be lifted to disk-bounded.

    [Inject] public required IDialogService DialogService { get; init; }

    /// <summary>
    /// Triggered when <see cref="EncryptionModel.PendingDownload"/>
    /// changes. Runs the file-download interop and signals completion via
    /// the supplied <see cref="TaskCompletionSource"/> so the originating
    /// command can finish its <c>StatusModel</c> update.
    ///
    /// <para>
    /// JSInterop lives in the consumer page partial, never the model —
    /// RxBlazorV2 §5 (Component Triggers) is the canonical seam for "model
    /// emits a side-effect, host runs interop and acks completion".
    /// </para>
    /// </summary>
    protected override Task OnPendingDownloadChangedAsync(CancellationToken cancellationToken)
    {
        if (Model.PendingDownload is not { } payload)
        {
            return Task.CompletedTask;
        }

        try
        {
            FileOperationsInterop.DownloadMessagePackFile(
                new ArraySegment<byte>(payload.Bytes),
                payload.FileName);
            payload.Done.TrySetResult();
        }
        catch (Exception ex)
        {
            payload.Done.TrySetException(ex);
        }
        finally
        {
            Model.PendingDownload = null;
        }
        return Task.CompletedTask;
    }

    /// <summary>
    /// Confirmation gate for the destructive <c>Reset</c> command. Wired
    /// into <c>MudButtonAsyncRx.ConfirmExecutionAsync</c>; returns
    /// <c>true</c> when the user confirms (the button then runs the
    /// command), <c>false</c> on cancel (the command never executes).
    /// </summary>
    private Task<bool> ConfirmResetAsync()
        => ConfirmDestructiveAsync(
            title: Model.Localizer["Btn_Reset"],
            message: Model.Localizer["Confirm_Reset"],
            destructiveLabel: Model.Localizer["Btn_Reset"]);

    /// <summary>
    /// Shared confirm-or-cancel dialog for destructive operations. Cancel
    /// is the visually-primary default (filled, primary color); the
    /// destructive action is colored red and outlined to mark it as the
    /// consequential, non-default choice. Returns <c>true</c> only when
    /// the user explicitly clicks the destructive button.
    /// </summary>
    private async Task<bool> ConfirmDestructiveAsync(string title, string message, string destructiveLabel)
    {
        var parameters = new DialogParameters<Components.DestructiveConfirmDialog>
        {
            { x => x.Title, title },
            { x => x.Message, message },
            { x => x.DestructiveLabel, destructiveLabel },
            { x => x.CancelLabel, Model.Localizer["Btn_Cancel"].ToString() },
        };
        var dialog = await DialogService.ShowAsync<Components.DestructiveConfirmDialog>(title, parameters);
        var result = await dialog.Result;
        return result is { Canceled: false, Data: true };
    }

    /// <summary>
    /// Unified file-pick handler for the import flow. Sniffs the picked
    /// file's extension and dispatches to the envelope (.eds → guided
    /// passkey-rebinding import via the streaming BlobSession path) or
    /// plain-ZIP (.zip → state-aware dispatch via
    /// <see cref="EncryptionModel.ImportAllDatabases"/>) path.
    ///
    /// <para>
    /// .eds is handed off as <see cref="IBrowserFile"/> — the model + service
    /// stream it into a JS-side BlobSession one chunk at a time, so the
    /// C# managed heap never holds the full envelope. .zip is still read
    /// into a managed byte[] (typical demo ZIPs are small enough); a
    /// browser-file variant for plain ZIPs is tracked as G8.7.
    /// </para>
    /// </summary>
    private async Task HandleImportPickedAsync(IBrowserFile? file)
    {
        if (file is null) return;

        if (file.Name.EndsWith(".eds", StringComparison.OrdinalIgnoreCase))
        {
            await HandleEnvelopeFileAsync(file);
        }
        else if (file.Name.EndsWith(".zip", StringComparison.OrdinalIgnoreCase))
        {
            var bytes = await ReadPickedAsync(file);
            if (bytes is null) return;
            await HandleZipBytesAsync(bytes);
        }
    }

    private async Task HandleEnvelopeFileAsync(IBrowserFile file)
    {
        var confirmed = await ConfirmDestructiveAsync(
            title: Model.Localizer["Btn_ImportDisk"],
            message: Model.Localizer["Confirm_ImportDisk"],
            destructiveLabel: Model.Localizer["Btn_ImportDisk"]);

        if (confirmed)
        {
            await Model.ImportDisk.ExecuteAsync(file);
        }
    }

    private async Task HandleZipBytesAsync(byte[] bytes)
    {
        // State-aware warning: a plain ZIP on a Locked disk breaks encryption,
        // on an Unlocked disk preserves it, on a Plain disk just replaces.
        // Session.ImportAllDatabasesAsync owns the dispatch; the page only
        // owns the right confirmation prompt for each outcome.
        var messageKey = Model switch
        {
            { IsLocked: true } => "Confirm_ImportAllDatabases_Locked",
            { IsUnlocked: true } => "Confirm_ImportAllDatabases_Unlocked",
            _ => "Confirm_ImportAllDatabases",
        };
        var confirmed = await ConfirmDestructiveAsync(
            title: Model.Localizer["Btn_ImportAllDatabases"],
            message: Model.Localizer[messageKey],
            destructiveLabel: Model.Localizer["Btn_ImportAllDatabases"]);

        if (confirmed)
        {
            await Model.ImportAllDatabases.ExecuteAsync(bytes);
        }
    }

    /// <summary>
    /// Common file-bytes read for the .zip plain-import path. Uses the
    /// picked file's own <see cref="IBrowserFile.Size"/> as the stream
    /// cap so legitimate multi-DB plain ZIPs aren't rejected by an
    /// arbitrary constant — the browser/picker already bounds what the
    /// user can hand in. Returns null on no-file-picked.
    /// </summary>
    private static async Task<byte[]?> ReadPickedAsync(IBrowserFile? file)
    {
        if (file is null) return null;
        await using var stream = file.OpenReadStream(maxAllowedSize: file.Size);
        using var ms = new MemoryStream(checked((int)file.Size));
        await stream.CopyToAsync(ms);
        return ms.ToArray();
    }
}
