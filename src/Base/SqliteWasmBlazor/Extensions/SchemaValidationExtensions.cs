using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace SqliteWasmBlazor;

/// <summary>
/// Raised by <see cref="SchemaValidationExtensions.ValidateImportedSchemaAsync"/>
/// when an imported database does not carry the tables the model expects —
/// the signature of a file that belongs to a different database.
///
/// <para>
/// <see cref="MissingTables"/> and <see cref="DatabaseDisplayName"/> are
/// the message: UI layers build their own localized sentence from them
/// rather than showing <see cref="Exception.Message"/>, which is English
/// and written for a developer reading a log.
/// </para>
/// </summary>
public sealed class SchemaMismatchException : InvalidOperationException
{
    /// <summary>
    /// Create a mismatch for <paramref name="databaseDisplayName"/> listing
    /// the tables the model requires and the file does not have.
    /// </summary>
    /// <param name="databaseDisplayName">Database the file was checked against, e.g. "NotesDb.db".</param>
    /// <param name="missingTables">Required tables absent from the file. Never empty.</param>
    public SchemaMismatchException(
        string databaseDisplayName,
        IReadOnlyList<string> missingTables)
        : base($"Incompatible database: missing tables {string.Join(", ", missingTables)}. " +
               $"The file is not a valid {databaseDisplayName} database.")
    {
        DatabaseDisplayName = databaseDisplayName;
        MissingTables = missingTables;
    }

    /// <summary>Database the file was checked against, e.g. <c>"NotesDb.db"</c>.</summary>
    public string DatabaseDisplayName { get; }

    /// <summary>Required tables the file does not contain.</summary>
    public IReadOnlyList<string> MissingTables { get; }
}

/// <summary>
/// Generic schema validation for any DbContext after raw database import.
/// Derives expected table names from the EF model metadata and checks sqlite_master.
/// </summary>
public static class SchemaValidationExtensions
{
    /// <summary>
    /// Validates that the database contains the tables defined in the EF model.
    /// Uses the design-time model to access IsTableExcludedFromMigrations
    /// (not available on the read-optimized runtime model).
    /// Skips owned entities and entities excluded from migrations (e.g., FTS5 virtual tables).
    /// </summary>
    /// <param name="context">The database context connected to the imported database.</param>
    /// <param name="databaseDisplayName">Display name for error messages (e.g., "TodoDb.db").</param>
    /// <exception cref="SchemaMismatchException">
    /// Thrown when required tables are missing. Carries the table names so a
    /// caller can phrase its own message in its own language.
    /// </exception>
    public static async Task ValidateImportedSchemaAsync(this DbContext context, string databaseDisplayName)
    {
        var designTimeModel = context.GetService<IDesignTimeModel>().Model;

        var requiredTables = designTimeModel.GetEntityTypes()
            .Where(e => !e.IsOwned()
                        && e.GetTableName() is not null
                        && !e.IsTableExcludedFromMigrations())
            .Select(e => e.GetTableName()!)
            .Distinct()
            .ToArray();

        var connection = context.Database.GetDbConnection();
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT name FROM sqlite_master WHERE type='table'";

        var tables = new HashSet<string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            tables.Add(reader.GetString(0));
        }

        var missingTables = requiredTables.Where(t => !tables.Contains(t)).ToArray();
        if (missingTables.Length > 0)
        {
            throw new SchemaMismatchException(databaseDisplayName, missingTables);
        }
    }
}
