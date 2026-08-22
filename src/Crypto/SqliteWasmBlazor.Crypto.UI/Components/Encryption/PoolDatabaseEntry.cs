namespace SqliteWasmBlazor.Crypto.UI.Components.Encryption;

/// <summary>
/// One row of the encryption panel's database list. The list is the union
/// of what the SAH pool currently holds and what the host declares it owns
/// (<see cref="IHostDatabaseService.OwnedDatabases"/>), so both
/// kinds of surprise are visible: a database the app expects but the pool
/// has lost, and a database sitting in the pool that nothing opens.
/// </summary>
/// <param name="Name">Pool file name, e.g. <c>TodoDb.db</c>.</param>
/// <param name="Owned">
/// True when the host opens this database by connection string. Owned rows
/// can be cleared but never removed — the app would query a hole. Unowned
/// rows are storage the app doesn't read; removing one is safe.
/// </param>
/// <param name="Present">
/// True when the pool currently holds the file. False only for an owned
/// database that has yet to be created — its row exists so a backup can be
/// imported into it before anything else touches it.
/// </param>
public sealed record PoolDatabaseEntry(string Name, bool Owned, bool Present);
