using System.Runtime.InteropServices.JavaScript;
using System.Runtime.Versioning;

namespace SqliteWasmBlazor.Components.Interop;

/// <summary>
/// Loads the Components file-operations JS module. Database downloads go
/// through <c>ISqliteWasmDatabaseService.ExportDatabaseToDownloadAsync</c>
/// (worker-staged, memory-flat); this module carries no byte-transfer
/// entry points.
/// </summary>
[SupportedOSPlatform("browser")]
public static partial class FileOperationsInterop
{
    private const string ModuleName = "SqliteWasmBlazor.Components.FileOperations";

    /// <summary>
    /// Initialize the file operations module.
    /// Must be called in Program.cs before WebAssemblyHostBuilder.Build().
    /// </summary>
    /// <param name="configure">Optional options callback. Override <see cref="SqliteWasmBlazor.Hosting.SqliteWasmAssetOptions.AssetRoot"/>
    /// for browser-extension builds.</param>
    public static async Task InitializeAsync(Action<SqliteWasmComponentsOptions>? configure = null)
    {
        if (!OperatingSystem.IsBrowser())
        {
            return;
        }

        var options = new SqliteWasmComponentsOptions();
        configure?.Invoke(options);

        try
        {
            await JSHost.ImportAsync(
                ModuleName,
                $"../{options.AssetRoot}file-operations.js");
            Console.WriteLine("FileOperations module loaded successfully");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error loading FileOperations module: {ex.Message}");
            throw;
        }
    }
}
