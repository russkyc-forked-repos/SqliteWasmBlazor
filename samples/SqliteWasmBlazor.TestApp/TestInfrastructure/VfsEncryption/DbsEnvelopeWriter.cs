namespace SqliteWasmBlazor.TestApp.TestInfrastructure.VfsEncryption;

/// <summary>
/// Writes the <c>.dbs</c> envelope the multi-DB import reads. Tests build
/// one directly because the export side ends in an anchor-click download,
/// which in-page test code cannot reach.
/// </summary>
internal static class DbsEnvelopeWriter
{
    /// <summary>
    /// Builds a <c>.dbs</c> envelope from a sequence of (name, bytes)
    /// pairs using just the MessagePack atoms the worker decoder reads:
    /// <c>array(N)</c>, <c>array(2)</c>, <c>str8</c>/<c>str16</c> for names,
    /// <c>bin32</c> for blobs. Mirrors the worker's
    /// <c>parseDbsHeader</c>/<c>readBinHeader</c> reader.
    /// </summary>
    internal static byte[] Build((string Name, byte[] Bytes)[] entries)
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
}
