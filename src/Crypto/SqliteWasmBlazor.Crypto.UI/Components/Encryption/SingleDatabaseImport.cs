using Microsoft.AspNetCore.Components.Forms;

namespace SqliteWasmBlazor.Crypto.UI.Components.Encryption;

/// <summary>
/// Payload for <see cref="EncryptionModel.ImportDatabase"/>: the picked
/// <c>.db</c> file plus the pool name it lands under. The name is explicit
/// because a file's name and a database's name are different things — an
/// export is stamped (<c>TodoDb-20260818-193737.db</c>) while the database
/// it came from is not. The name comes from the row the user picked the
/// file on, so an import always lands on a database that already has a
/// meaning in this app rather than creating a stray nothing opens.
/// </summary>
/// <param name="File">Picked file, streamed chunk-wise into the worker.</param>
/// <param name="DatabaseName">Pool name to create or replace.</param>
public sealed record SingleDatabaseImport(IBrowserFile File, string DatabaseName);