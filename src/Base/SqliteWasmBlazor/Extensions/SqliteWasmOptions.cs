// SqliteWasmBlazor - Minimal EF Core compatible provider
// MIT License

using SqliteWasmBlazor.Hosting;

namespace SqliteWasmBlazor;

/// <summary>
/// Configuration for SqliteWasmBlazor worker and asset resolution.
/// Registered via <see cref="SqliteWasmServiceCollectionExtensions.AddSqliteWasm(Microsoft.Extensions.DependencyInjection.IServiceCollection, System.Action{SqliteWasmOptions}?)"/>.
/// </summary>
public sealed class SqliteWasmOptions : SqliteWasmAssetOptions
{
    /// <summary>
    /// Creates the options with <c>AssetRoot</c> pointing at this package's
    /// static web assets. Override it only for a host that serves them from
    /// somewhere else, such as a browser-extension build.
    /// </summary>
    public SqliteWasmOptions()
    {
        AssetRoot = "_content/SqliteWasmBlazor/";
    }

    /// <summary>
    /// Enables logging of executed SQL commands and parameters to the browser console.
    /// This is disabled by default to prevent leaking sensitive application schema or data.
    /// </summary>
    public bool EnableCommandSqlLogging { get; set; }
}
