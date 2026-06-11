using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Forms;
using MudBlazor;
using SqliteWasmBlazor.Crypto.UI.Components.Encryption;

namespace SqliteWasmBlazor.Demo.Pages;

public partial class DatabaseEncryption
{
    [Inject] public required IDialogService DialogService { get; init; }

    /// <summary>
    /// Toggle a DB name in the model's
    /// <see cref="EncryptionModel.SelectedDatabases"/> list. Driven by the
    /// per-DB checkboxes in the export card; ObservableList's Add / Remove
    /// emit change notifications natively so the command's CanExecute
    /// re-evaluates without a manual reassign.
    /// </summary>
    private void OnDatabaseSelectionChanged(string dbName, bool isSelected)
    {
        if (isSelected)
        {
            if (!Model.SelectedDatabases.Contains(dbName))
            {
                Model.SelectedDatabases.Add(dbName);
            }
        }
        else
        {
            Model.SelectedDatabases.Remove(dbName);
        }
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
    /// file's extension and routes:
    /// <list type="bullet">
    ///   <item><c>.eds</c> → guided passkey-rebinding disk import
    ///   (<see cref="EncryptionModel.ImportDisk"/>).</item>
    ///   <item><c>.db</c> or <c>.dbs</c> → plain single-DB write or pool
    ///   replace, both via <see cref="EncryptionModel.ImportDatabases"/>;
    ///   the model owns the extension dispatch and streams the file into
    ///   the JS-side BlobSession one chunk at a time.</item>
    /// </list>
    /// All three paths are stream-shaped — the C# managed heap never
    /// holds the full envelope/file. The confirmation prompt is the
    /// page's job; the destructive scope (one DB vs whole pool vs the
    /// whole disk + credential) decides the wording.
    /// </summary>
    private async Task HandleImportPickedAsync(IBrowserFile? file)
    {
        if (file is null) { return; }

        if (file.Name.EndsWith(".eds", StringComparison.OrdinalIgnoreCase))
        {
            await HandleEnvelopeFileAsync(file);
        }
        else if (file.Name.EndsWith(".dbs", StringComparison.OrdinalIgnoreCase))
        {
            await HandleDbsFileAsync(file);
        }
        else if (file.Name.EndsWith(".db", StringComparison.OrdinalIgnoreCase))
        {
            await HandleSingleDbFileAsync(file);
        }
    }

    private async Task HandleSingleDbFileAsync(IBrowserFile file)
    {
        var confirmed = await ConfirmDestructiveAsync(
            title: Model.Localizer["Btn_ImportFile"],
            message: Model.Localizer["Confirm_ImportSingleDatabase"],
            destructiveLabel: Model.Localizer["Btn_ImportFile"]);

        if (confirmed)
        {
            await Model.ImportDatabases.ExecuteAsync(file);
        }
    }

    private async Task HandleDbsFileAsync(IBrowserFile file)
    {
        // Multi-DB envelope replaces the entire pool. On Unlocked disks
        // the worker rekey-on-writes each entry under globalKey; on Plain
        // disks it writes plain pages. CanImportDatabases gates Locked
        // out (no key to encrypt under).
        var messageKey = Model.IsUnlocked
            ? "Confirm_ImportDatabases_Unlocked"
            : "Confirm_ImportDatabases";
        var confirmed = await ConfirmDestructiveAsync(
            title: Model.Localizer["Btn_ImportDatabases"],
            message: Model.Localizer[messageKey],
            destructiveLabel: Model.Localizer["Btn_ImportDatabases"]);

        if (confirmed)
        {
            await Model.ImportDatabases.ExecuteAsync(file);
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
}
