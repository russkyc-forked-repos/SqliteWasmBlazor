// SqliteWasmBlazor - Minimal EF Core compatible provider
// MIT License

using System.Collections;
using System.Data.Common;

namespace SqliteWasmBlazor;

/// <summary>
/// DataReader that wraps results from sqlite-wasm worker.
/// </summary>
public sealed class SqliteWasmDataReader : DbDataReader
{
    private readonly SqlQueryResult _result;
    private int _currentRowIndex = -1;
    private bool _isClosed;

    internal SqliteWasmDataReader(SqlQueryResult result)
    {
        _result = result;
    }

    /// <inheritdoc />
    public override T GetFieldValue<T>(int ordinal)
    {
        // Special handling for DateTimeOffset since there's no GetDateTimeOffset() in DbDataReader
        if (typeof(T) == typeof(DateTimeOffset))
        {
            return (T)(object)GetDateTimeOffset(ordinal);
        }

        // Special handling for TimeSpan
        if (typeof(T) == typeof(TimeSpan))
        {
            return (T)(object)GetTimeSpan(ordinal);
        }

        // Default behavior for all other types
        return base.GetFieldValue<T>(ordinal);
    }

    /// <inheritdoc />
    public override int Depth => 0;

    /// <inheritdoc />
    public override int FieldCount => _result.ColumnNames.Count;

    /// <inheritdoc />
    public override bool HasRows => _result.Rows.Length > 0;

    /// <inheritdoc />
    public override bool IsClosed => _isClosed;

    /// <inheritdoc />
    public override int RecordsAffected => _result.RowsAffected;

    /// <inheritdoc />
    public override object this[int ordinal] => GetValue(ordinal);

    /// <inheritdoc />
    public override object this[string name] => GetValue(GetOrdinal(name));

    /// <inheritdoc />
    public override bool GetBoolean(int ordinal)
    {
        var value = GetValue(ordinal);
        return Convert.ToBoolean(value);
    }

    /// <inheritdoc />
    public override byte GetByte(int ordinal)
    {
        var value = GetValue(ordinal);
        return Convert.ToByte(value);
    }

    /// <inheritdoc />
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        var value = GetValue(ordinal);
        byte[] bytes;

        if (value is byte[] byteArray)
        {
            bytes = byteArray;
        }
        else
        {
            throw new InvalidCastException($"Column {ordinal} is not a byte array.");
        }

        if (buffer == null)
        {
            return bytes.Length;
        }

        var bytesToCopy = Math.Min(length, bytes.Length - (int)dataOffset);
        Array.Copy(bytes, dataOffset, buffer, bufferOffset, bytesToCopy);
        return bytesToCopy;
    }

    /// <inheritdoc />
    public override char GetChar(int ordinal)
    {
        var value = GetValue(ordinal);

        // Handle single-character string (match Microsoft.Data.Sqlite behavior)
        if (value is string str)
        {
            if (str.Length == 1)
            {
                return str[0];
            }
            // For multi-char or empty strings, fall through to numeric conversion
        }

        return Convert.ToChar(value);
    }

    /// <inheritdoc />
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        var value = GetString(ordinal);
        if (buffer == null)
        {
            return value.Length;
        }

        var charsToCopy = Math.Min(length, value.Length - (int)dataOffset);
        value.CopyTo((int)dataOffset, buffer, bufferOffset, charsToCopy);
        return charsToCopy;
    }

    /// <inheritdoc />
    public override string GetDataTypeName(int ordinal)
    {
        return _result.ColumnTypes[ordinal];
    }

    /// <inheritdoc />
    public override DateTime GetDateTime(int ordinal)
    {
        var value = GetValue(ordinal);
        if (value is DateTime dt)
        {
            return dt;
        }
        if (value is string str)
        {
            return DateTime.Parse(str, null, System.Globalization.DateTimeStyles.RoundtripKind);
        }
        throw new InvalidCastException($"Column {ordinal} is not a DateTime. Actual type: {value.GetType().Name}");
    }

    /// <summary>
    /// Reads a column as <see cref="DateTimeOffset"/>. Not an override —
    /// <see cref="DbDataReader"/> has no such method, and SQLite has no
    /// native type for it. Accepts a stored <see cref="DateTimeOffset"/>, a
    /// <see cref="DateTime"/>, or TEXT, which is how the value is normally
    /// stored; TEXT is parsed as universal time, matching
    /// Microsoft.Data.Sqlite.
    /// </summary>
    /// <param name="ordinal">Zero-based column index.</param>
    /// <exception cref="InvalidCastException">
    /// The column holds none of those forms.
    /// </exception>
    public DateTimeOffset GetDateTimeOffset(int ordinal)
    {
        var value = GetValue(ordinal);
        if (value is DateTimeOffset dto)
        {
            return dto;
        }
        if (value is DateTime dt)
        {
            return new DateTimeOffset(dt);
        }
        if (value is string str)
        {
            // Handle TEXT storage (SQLite stores DateTimeOffset as TEXT)
            // Match Microsoft.Data.Sqlite behavior
            return DateTimeOffset.Parse(str, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AssumeUniversal);
        }
        throw new InvalidCastException($"Column {ordinal} is not a DateTimeOffset or DateTime. Actual type: {value.GetType().Name}");
    }

    /// <summary>
    /// Reads a column as <see cref="TimeSpan"/>. Not an override —
    /// <see cref="DbDataReader"/> has no such method. A numeric column is
    /// read as <b>days</b>, not milliseconds, which is what
    /// Microsoft.Data.Sqlite does and the one detail worth checking if a
    /// duration comes back wrong by a large factor.
    /// </summary>
    /// <param name="ordinal">Zero-based column index.</param>
    /// <exception cref="InvalidCastException">
    /// The column holds a form that cannot be read as a duration.
    /// </exception>
    public TimeSpan GetTimeSpan(int ordinal)
    {
        var value = GetValue(ordinal);
        if (value is TimeSpan ts)
        {
            return ts;
        }
        // Match Microsoft.Data.Sqlite behavior: FLOAT/INTEGER stored as days (not milliseconds!)
        if (value is double d)
        {
            return TimeSpan.FromDays(d);
        }
        if (value is long l)
        {
            return TimeSpan.FromDays(l);
        }
        if (value is int i)
        {
            return TimeSpan.FromDays(i);
        }
        if (value is string str)
        {
            return TimeSpan.Parse(str);
        }
        throw new InvalidCastException($"Column {ordinal} is not a TimeSpan. Actual type: {value.GetType().Name}");
    }

    /// <inheritdoc />
    public override decimal GetDecimal(int ordinal)
    {
        var value = GetValue(ordinal);
        if (value is string str)
        {
            return decimal.Parse(str, System.Globalization.CultureInfo.InvariantCulture);
        }
        return Convert.ToDecimal(value);
    }

    /// <inheritdoc />
    public override double GetDouble(int ordinal)
    {
        var value = GetValue(ordinal);
        return Convert.ToDouble(value);
    }

    /// <inheritdoc />
    public override Type GetFieldType(int ordinal)
    {
        if (_currentRowIndex < 0 || _currentRowIndex >= _result.Rows.Length)
        {
            // Return string as default if no data yet
            return typeof(string);
        }

        var value = _result.Rows[_currentRowIndex][ordinal];
        return value?.GetType() ?? typeof(object);
    }

    /// <inheritdoc />
    public override float GetFloat(int ordinal)
    {
        var value = GetValue(ordinal);
        return Convert.ToSingle(value);
    }

    /// <inheritdoc />
    public override Guid GetGuid(int ordinal)
    {
        var value = GetValue(ordinal);
        if (value is Guid guid)
        {
            return guid;
        }
        if (value is string str)
        {
            return Guid.Parse(str);
        }
        if (value is byte[] bytes)
        {
            // Match Microsoft.Data.Sqlite behavior:
            // If 16 bytes, interpret as Guid directly
            // Otherwise, interpret as UTF-8 encoded Guid string
            return bytes.Length == 16
                ? new Guid(bytes)
                : new Guid(System.Text.Encoding.UTF8.GetString(bytes));
        }
        throw new InvalidCastException($"Column {ordinal} is not a Guid. Actual type: {value.GetType().Name}");
    }

    /// <inheritdoc />
    public override short GetInt16(int ordinal)
    {
        var value = GetValue(ordinal);
        return Convert.ToInt16(value);
    }

    /// <inheritdoc />
    public override int GetInt32(int ordinal)
    {
        var value = GetValue(ordinal);
        return Convert.ToInt32(value);
    }

    /// <inheritdoc />
    public override long GetInt64(int ordinal)
    {
        var value = GetValue(ordinal);
        return Convert.ToInt64(value);
    }

    /// <inheritdoc />
    public override string GetName(int ordinal)
    {
        if (ordinal < 0 || ordinal >= _result.ColumnNames.Count)
        {
            throw new ArgumentOutOfRangeException(nameof(ordinal));
        }
        return _result.ColumnNames[ordinal];
    }

    /// <inheritdoc />
    public override int GetOrdinal(string name)
    {
        var index = _result.ColumnNames.IndexOf(name);
        if (index < 0)
        {
            throw new ArgumentException($"Column '{name}' not found.", nameof(name));
        }
        return index;
    }

    /// <inheritdoc />
    public override string GetString(int ordinal)
    {
        var value = GetValue(ordinal);
        return Convert.ToString(value) ?? string.Empty;
    }

    /// <inheritdoc />
    public override object GetValue(int ordinal)
    {
        if (_currentRowIndex < 0 || _currentRowIndex >= _result.Rows.Length)
        {
            throw new InvalidOperationException("No current row.");
        }

        if (ordinal < 0 || ordinal >= _result.Rows[_currentRowIndex].Length)
        {
            throw new ArgumentOutOfRangeException(nameof(ordinal));
        }

        var value = _result.Rows[_currentRowIndex][ordinal];

        if (value is null)
        {
            return DBNull.Value;
        }

        return value;
    }

    /// <inheritdoc />
    public override int GetValues(object[] values)
    {
        if (_currentRowIndex < 0 || _currentRowIndex >= _result.Rows.Length)
        {
            throw new InvalidOperationException("No current row.");
        }

        var count = Math.Min(values.Length, FieldCount);
        for (var i = 0; i < count; i++)
        {
            values[i] = GetValue(i);
        }
        return count;
    }

    /// <inheritdoc />
    public override bool IsDBNull(int ordinal)
    {
        var value = GetValue(ordinal);
        return value is DBNull;
    }

    /// <inheritdoc />
    public override bool NextResult()
    {
        // SQLite doesn't support multiple result sets
        return false;
    }

    /// <inheritdoc />
    public override bool Read()
    {
        if (_isClosed)
        {
            throw new InvalidOperationException("DataReader is closed.");
        }

        _currentRowIndex++;
        return _currentRowIndex < _result.Rows.Length;
    }

    /// <inheritdoc />
    public override IEnumerator GetEnumerator()
    {
        return new DbEnumerator(this, closeReader: false);
    }

    /// <inheritdoc />
    public override void Close()
    {
        _isClosed = true;
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Close();
        }
        base.Dispose(disposing);
    }
}
