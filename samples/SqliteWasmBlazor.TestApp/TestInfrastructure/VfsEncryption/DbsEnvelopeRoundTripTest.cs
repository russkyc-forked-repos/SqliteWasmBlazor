using Microsoft.EntityFrameworkCore;

namespace SqliteWasmBlazor.TestApp.TestInfrastructure.VfsEncryption;

/// <summary>
/// Streaming multi-DB <c>.dbs</c> envelope round-trip. The page uses the
/// envelope when ≥ 2 DBs are picked in the checkbox export selector;
/// this test exercises the import side end-to-end (worker pool wipe +
/// MessagePack envelope parse + chunked SAH writes per entry).
///
/// <para>
/// Building the envelope in C# avoids the export-side anchor-click
/// download which isn't reachable from in-page test code. Bytes for both
/// entries come from a single EF-populated DB exported through
/// <see cref="ISqliteWasmDatabaseService.ExportDatabaseAsync"/>; the
/// envelope packs them under two distinct names. After import, both
/// names must appear in the pool with byte-for-byte equal contents.
/// </para>
/// </summary>
internal sealed class DbsEnvelopeRoundTripTest
{
    private const int RowCount = 8;
    private const string EntryA = "DbsTest-A.db";
    private const string EntryB = "DbsTest-B.db";

    private readonly IDbContextFactory<PrfVfsTestContext> _factory;
    private readonly ISqliteWasmDatabaseService _databaseService;
    private readonly IEncryptedSqliteWasmDatabaseService _session;

    public string Name => "Plain_DbsEnvelope_StreamingImport_RoundTrip";

    public DbsEnvelopeRoundTripTest(
        IDbContextFactory<PrfVfsTestContext> factory,
        ISqliteWasmDatabaseService databaseService,
        IEncryptedSqliteWasmDatabaseService session)
    {
        _factory = factory;
        _databaseService = databaseService;
        _session = session;
    }

    public async ValueTask<string?> RunAsync()
    {
        await CleanupAsync();

        // ---- Populate the EF context, snapshot its plain bytes ------------
        await using (var ctx = await _factory.CreateDbContextAsync())
        {
            await ctx.Database.EnsureCreatedAsync();
            for (var i = 0; i < RowCount; i++)
            {
                ctx.Items.Add(new VfsTestItem
                {
                    Marker = $"dbs-{i}",
                    Payload = $"payload-{i}-{Guid.NewGuid():N}",
                });
            }
            await ctx.SaveChangesAsync();
        }

        byte[] plainBytes;
        try
        {
            plainBytes = await _databaseService.ExportDatabaseAsync(PrfVfsTestContext.DatabaseName);
        }
        catch (Exception ex)
        {
            return $"FAIL[ExportDatabaseAsync]: {ex.GetType().Name}: {ex.Message}";
        }
        if (plainBytes.Length == 0)
        {
            return "FAIL: ExportDatabaseAsync returned empty bytes";
        }

        // ---- Wipe the pool so the importer has no pre-existing files ------
        await CleanupAsync();

        // ---- Build a .dbs envelope: array<2>[[name, bytes], [name, bytes]]
        var envelope = BuildDbsEnvelope(new[]
        {
            (EntryA, plainBytes),
            (EntryB, plainBytes),
        });

        // ---- Import via the streaming entry point -------------------------
        try
        {
            using var stream = new MemoryStream(envelope, writable: false);
            await _session.ImportDatabasesFromStreamAsync(stream, envelope.Length);
        }
        catch (Exception ex)
        {
            return $"FAIL[ImportDatabasesFromStreamAsync]: {ex.GetType().Name}: {ex.Message}";
        }

        // ---- Verify both entries landed -----------------------------------
        var names = await _databaseService.ListDatabasesAsync();
        if (!names.Contains(EntryA))
        {
            return $"FAIL: pool missing '{EntryA}' after import (got [{string.Join(", ", names)}])";
        }
        if (!names.Contains(EntryB))
        {
            return $"FAIL: pool missing '{EntryB}' after import (got [{string.Join(", ", names)}])";
        }

        var a = await _databaseService.ExportDatabaseAsync(EntryA);
        var b = await _databaseService.ExportDatabaseAsync(EntryB);
        if (!a.AsSpan().SequenceEqual(plainBytes))
        {
            return $"FAIL[{EntryA}]: bytes differ ({a.Length} vs {plainBytes.Length})";
        }
        if (!b.AsSpan().SequenceEqual(plainBytes))
        {
            return $"FAIL[{EntryB}]: bytes differ ({b.Length} vs {plainBytes.Length})";
        }

        await CleanupAsync();
        return "OK";
    }

    /// <summary>
    /// Builds a v3 <c>.dbs</c> envelope from a sequence of (name, bytes)
    /// pairs using just the MessagePack atoms the worker decoder reads:
    /// <c>array(N)</c>, <c>array(2)</c>, <c>str8</c>/<c>str16</c> for names,
    /// <c>bin32</c> for blobs. Mirrors the worker's
    /// <c>parseDbsHeader</c>/<c>readBinHeader</c> reader.
    /// </summary>
    private static byte[] BuildDbsEnvelope((string Name, byte[] Bytes)[] entries)
    {
        using var ms = new MemoryStream();
        WriteArrayHeader(ms, entries.Length);
        foreach (var (name, bytes) in entries)
        {
            WriteArrayHeader(ms, 2);
            WriteString(ms, name);
            WriteBin32(ms, bytes);
        }
        return ms.ToArray();
    }

    private static void WriteArrayHeader(Stream s, int count)
    {
        if (count < 16)
        {
            s.WriteByte((byte)(0x90 | count));
            return;
        }
        if (count <= 0xFFFF)
        {
            s.WriteByte(0xDC);
            s.WriteByte((byte)(count >> 8));
            s.WriteByte((byte)count);
            return;
        }
        s.WriteByte(0xDD);
        s.WriteByte((byte)(count >> 24));
        s.WriteByte((byte)(count >> 16));
        s.WriteByte((byte)(count >> 8));
        s.WriteByte((byte)count);
    }

    private static void WriteString(Stream s, string value)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(value);
        if (bytes.Length < 32)
        {
            s.WriteByte((byte)(0xA0 | bytes.Length));
        }
        else if (bytes.Length <= 0xFF)
        {
            s.WriteByte(0xD9);
            s.WriteByte((byte)bytes.Length);
        }
        else if (bytes.Length <= 0xFFFF)
        {
            s.WriteByte(0xDA);
            s.WriteByte((byte)(bytes.Length >> 8));
            s.WriteByte((byte)bytes.Length);
        }
        else
        {
            s.WriteByte(0xDB);
            s.WriteByte((byte)(bytes.Length >> 24));
            s.WriteByte((byte)(bytes.Length >> 16));
            s.WriteByte((byte)(bytes.Length >> 8));
            s.WriteByte((byte)bytes.Length);
        }
        s.Write(bytes, 0, bytes.Length);
    }

    private static void WriteBin32(Stream s, byte[] bytes)
    {
        s.WriteByte(0xC6);
        s.WriteByte((byte)(bytes.Length >> 24));
        s.WriteByte((byte)(bytes.Length >> 16));
        s.WriteByte((byte)(bytes.Length >> 8));
        s.WriteByte((byte)bytes.Length);
        s.Write(bytes, 0, bytes.Length);
    }

    private async Task CleanupAsync()
    {
        try { await _session.ResetPoolAsync(); } catch { }
        try
        {
            var names = await _databaseService.ListDatabasesAsync();
            foreach (var n in names)
            {
                try { await _databaseService.DeleteDatabaseAsync(n); } catch { }
            }
        }
        catch { }
    }
}
