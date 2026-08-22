namespace SqliteWasmBlazor.TestApp.TestInfrastructure;

/// <summary>
/// Bytes-in-hand helpers over the streamed file paths, for tests that need a
/// <c>byte[]</c> to assert on or to feed back in.
///
/// <para>
/// The library deliberately has no such method: every public file path is
/// memory-flat so a large database transfers on a phone, and materializing is
/// the caller's explicit choice. A test is the caller for whom that choice is
/// right — the databases here are small and the assertions are about the
/// bytes themselves.
/// </para>
///
/// <para>
/// These emit and read <em>plain</em> pages, the same as the download paths.
/// A test that needs the physical on-disk bytes — ciphertext on an encrypted
/// pool — wants <c>SqliteWasmWorkerBridge.Instance.ExportDatabaseRawAsync</c>
/// instead, and its opaque counterpart for writing them back.
/// </para>
/// </summary>
internal static class DatabaseBytesExtensions
{
    /// <summary>Drain a database's plain bytes into a <c>byte[]</c>.</summary>
    public static async Task<byte[]> ExportDatabaseBytesAsync(
        this ISqliteWasmDatabaseService service,
        string databaseName,
        CancellationToken cancellationToken = default)
    {
        using var buffer = new MemoryStream();
        await service.ExportDatabaseToStreamAsync(databaseName, buffer, cancellationToken);
        return buffer.ToArray();
    }

    /// <summary>
    /// Import a plain <c>.db</c> held in memory. Signals by exception, like
    /// the streamed path it wraps.
    /// </summary>
    public static async Task ImportDatabaseBytesAsync(
        this ISqliteWasmDatabaseService service,
        string databaseName,
        byte[] data,
        CancellationToken cancellationToken = default)
    {
        using var source = new MemoryStream(data, writable: false);
        await service.ImportDatabaseFromStreamAsync(
            databaseName, source, data.Length, null, cancellationToken);
    }
}
