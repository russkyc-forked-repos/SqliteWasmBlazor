using Microsoft.AspNetCore.Components.Forms;

namespace SqliteWasmBlazor.Crypto.UI.Components.Encryption;

/// <summary>
/// Payload for <see cref="EncryptionModel.ImportDatabase"/>: the picked
/// <c>.db</c> file plus the pool name it lands under. The name is explicit
/// because a file's name and a database's name are different things — an
/// export is stamped (<c>TodoDb-20260818-193737.db</c>) while the database
/// it came from is not, and importing under the file name would leave a
/// stamped stray in the pool instead of restoring the original.
/// <see cref="EncryptionModel.ProposeDatabaseName"/> supplies the default
/// the host prefills its name field with.
/// </summary>
/// <param name="File">Picked file, streamed chunk-wise into the worker.</param>
/// <param name="DatabaseName">Pool name to create or replace.</param>
public sealed record SingleDatabaseImport(IBrowserFile File, string DatabaseName);
