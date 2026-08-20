using MudBlazor;

namespace SqliteWasmBlazor.Demo.Components;

/// <summary>
/// Confirm-or-cancel gate in front of every destructive action in the demo
/// (full reset, replace-all imports, per-database delete). Lives here rather
/// than on a page because two pages need it and the dialog's safety choices —
/// cancel as the primary default, destructive in red — should not be
/// re-decided per callsite. See <see cref="DestructiveConfirmDialog"/> for
/// those choices.
/// </summary>
internal static class DestructiveConfirm
{
    /// <summary>
    /// Shows the dialog and returns <c>true</c> only when the user explicitly
    /// clicks the destructive button — cancel, dismiss and backdrop-click all
    /// return <c>false</c>.
    /// </summary>
    public static async Task<bool> ConfirmDestructiveAsync(
        this IDialogService dialogService,
        string title,
        string message,
        string destructiveLabel,
        string cancelLabel)
    {
        var parameters = new DialogParameters<DestructiveConfirmDialog>
        {
            { x => x.Title, title },
            { x => x.Message, message },
            { x => x.DestructiveLabel, destructiveLabel },
            { x => x.CancelLabel, cancelLabel },
        };

        var dialog = await dialogService.ShowAsync<DestructiveConfirmDialog>(title, parameters);
        var result = await dialog.Result;
        return result is { Canceled: false, Data: true };
    }
}
