using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Forms;
using MudBlazor;
using SqliteWasmBlazor.Crypto.UI.Components.Encryption;
using SqliteWasmBlazor.Demo.Components;

namespace SqliteWasmBlazor.Demo.Pages;

public partial class DatabaseEncryption
{
    [Inject]
    public required IDialogService DialogService { get; init; }

    /// <summary>
    /// Toggle a DB name in the model's
    /// <see cref="EncryptionModel.SelectedDatabases"/> list. Driven by the
    /// per-DB checkboxes in the database list; ObservableList's Add / Remove
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
    /// Page-local shorthand over <see cref="DestructiveConfirm"/> — supplies
    /// this model's cancel label so the callsites below stay one line each.
    /// </summary>
    private Task<bool> ConfirmDestructiveAsync(string title, string message, string destructiveLabel)
        => DialogService.ConfirmDestructiveAsync(
            title, message, destructiveLabel, Model.Localizer["Btn_Cancel"]);

    /// <summary>
    /// A <c>.db</c> file picked on one database's row. The row decides the
    /// target, so the only question left is whether the user means to
    /// overwrite what that database currently holds.
    /// </summary>
    private async Task HandleRowFilePickedAsync(PoolDatabaseEntry entry, IBrowserFile? file)
    {
        if (file is null)
        {
            return;
        }

        var confirmed = await ConfirmDestructiveAsync(
            title: Model.Localizer["Btn_ImportIntoDatabase", entry.Name],
            message: Model.Localizer[
                Model.IsUnlocked ? "Confirm_ImportSingleDatabase_Unlocked" : "Confirm_ImportSingleDatabase",
                entry.Name, file.Name],
            destructiveLabel: Model.Localizer["Btn_Import"]);

        if (confirmed)
        {
            await Model.ImportDatabase.ExecuteAsync(new SingleDatabaseImport(file, entry.Name));
        }
    }

    /// <summary>
    /// Confirmation gate for the per-row clear/remove action. An owned
    /// database comes back empty (the app opens it by connection string, so
    /// it cannot be left missing); an unowned one is gone for good. The
    /// wording has to say which of the two is about to happen.
    /// </summary>
    private Task<bool> ConfirmClearDatabaseAsync(PoolDatabaseEntry entry)
        => entry.Owned
            ? ConfirmDestructiveAsync(
                title: Model.Localizer["Btn_ClearDatabase"],
                message: Model.Localizer["Confirm_ClearDatabase", entry.Name],
                destructiveLabel: Model.Localizer["Btn_ClearDatabase"])
            : ConfirmDestructiveAsync(
                title: Model.Localizer["Btn_DeleteDatabase"],
                message: Model.Localizer["Confirm_DeleteDatabase", entry.Name],
                destructiveLabel: Model.Localizer["Btn_DeleteDatabase"]);

    /// <summary>
    /// A file picked in the replace-everything card. Both accepted formats
    /// wipe the pool; the extension decides how much else goes with it:
    /// <list type="bullet">
    ///   <item><c>.dbs</c> — pool content only, encryption and passkey stay
    ///   as they are.</item>
    ///   <item><c>.eds</c> — pool content plus the passkey binding, which
    ///   ends the current session and signs the user back in as whoever the
    ///   envelope was made for.</item>
    /// </list>
    /// Anything else was picked through a file dialog that ignored the
    /// accept list; say so rather than letting a format check deep in the
    /// import path phrase it.
    /// </summary>
    private async Task HandleReplaceAllFilePickedAsync(IBrowserFile? file)
    {
        if (file is null)
        {
            return;
        }

        if (file.Name.EndsWith(".eds", StringComparison.OrdinalIgnoreCase))
        {
            await HandleEnvelopeFileAsync(file);
        }
        else if (file.Name.EndsWith(".dbs", StringComparison.OrdinalIgnoreCase))
        {
            await HandleDbsFileAsync(file);
        }
        else
        {
            Model.StatusModel.AddWarning(
                Model.Localizer["Error_UnsupportedImportFile", file.Name],
                nameof(Model.ImportPool));
        }
    }

    private async Task HandleDbsFileAsync(IBrowserFile file)
    {
        // Multi-DB envelope replaces the entire pool. On an Unlocked pool
        // the worker rekey-on-writes each entry under globalKey; on Plain
        // pools it writes plain pages. CanImportDatabases gates Locked
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
        // On an unlocked pool the command locks the session before it
        // starts, so the confirmation has to cover the sign-out too.
        var messageKey = Model.IsUnlocked
            ? "Confirm_ImportPool_Unlocked"
            : "Confirm_ImportPool";
        var confirmed = await ConfirmDestructiveAsync(
            title: Model.Localizer["Btn_ImportPool"],
            message: Model.Localizer[messageKey],
            destructiveLabel: Model.Localizer["Btn_ImportPool"]);

        if (confirmed)
        {
            await Model.ImportPool.ExecuteAsync(file);
        }
    }
}