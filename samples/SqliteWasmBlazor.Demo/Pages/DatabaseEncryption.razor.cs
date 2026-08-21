using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Forms;
using MudBlazor;
using SqliteWasmBlazor.Crypto.UI.Components.Encryption;
using SqliteWasmBlazor.Demo.Components;

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
    /// Page-local shorthand over <see cref="DestructiveConfirm"/> — supplies
    /// this model's cancel label so the callsites below stay one line each.
    /// </summary>
    private Task<bool> ConfirmDestructiveAsync(string title, string message, string destructiveLabel)
        => DialogService.ConfirmDestructiveAsync(
            title, message, destructiveLabel, Model.Localizer["Btn_Cancel"]);

    /// <summary>
    /// Unified file-pick handler for the import flow. Sniffs the picked
    /// file's extension and routes:
    /// <list type="bullet">
    ///   <item><c>.eds</c> → guided passkey-rebinding disk import
    ///   (<see cref="EncryptionModel.ImportPool"/>).</item>
    ///   <item><c>.db</c> → single-DB write into a database the user names
    ///   (<see cref="EncryptionModel.ImportDatabase"/>).</item>
    ///   <item><c>.dbs</c> → bundle import that replaces the pool
    ///   (<see cref="EncryptionModel.ImportDatabases"/>).</item>
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

    /// <summary>
    /// Single-DB import: ask which database the file lands in before
    /// writing. The field starts at <see cref="EncryptionModel.ProposeDatabaseName"/>
    /// (the file name minus our export stamp) so the common case — restore
    /// what was exported — is one confirm away, while an import into a
    /// different or new database stays one edit away.
    /// </summary>
    private async Task HandleSingleDbFileAsync(IBrowserFile file)
    {
        var title = Model.Localizer["Btn_ImportDatabase"].ToString();
        var parameters = new DialogParameters<Components.DatabaseNameDialog>
        {
            { x => x.Message, Model.Localizer["Confirm_ImportSingleDatabase"].ToString() },
            { x => x.Label, Model.Localizer["Lbl_ImportTargetName"].ToString() },
            { x => x.HelperText, Model.Localizer["Hint_ImportTargetName"].ToString() },
            { x => x.InitialName, Model.ProposeDatabaseName(file.Name) },
            { x => x.ExistingSummary, ExistingDatabasesSummary() },
            { x => x.ConfirmLabel, Model.Localizer["Btn_Import"].ToString() },
            { x => x.CancelLabel, Model.Localizer["Btn_Cancel"].ToString() },
        };
        var dialog = await DialogService.ShowAsync<Components.DatabaseNameDialog>(title, parameters);
        var result = await dialog.Result;
        if (result is { Canceled: false, Data: string target } && target.Length > 0)
        {
            await Model.ImportDatabase.ExecuteAsync(new SingleDatabaseImport(file, target));
        }
    }

    private string? ExistingDatabasesSummary()
        => Model.DatabaseNames.Count == 0
            ? null
            : Model.Localizer["Lbl_ExistingDatabases", string.Join(", ", Model.DatabaseNames)].ToString();

    /// <summary>
    /// Confirmation gate for the per-database delete action in the export
    /// picker. Wired into <c>MudIconButtonAsyncRxOf.ConfirmExecutionAsync</c>;
    /// the database name is in the message because the list is the only
    /// place the user sees which entries exist.
    /// </summary>
    private Task<bool> ConfirmDeleteDatabaseAsync(string dbName)
        => ConfirmDestructiveAsync(
            title: Model.Localizer["Btn_DeleteDatabase"],
            message: Model.Localizer["Confirm_DeleteDatabase", dbName],
            destructiveLabel: Model.Localizer["Btn_DeleteDatabase"]);

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
            title: Model.Localizer["Btn_ImportPool"],
            message: Model.Localizer["Confirm_ImportPool"],
            destructiveLabel: Model.Localizer["Btn_ImportPool"]);

        if (confirmed)
        {
            await Model.ImportPool.ExecuteAsync(file);
        }
    }
}
