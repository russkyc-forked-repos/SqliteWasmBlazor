using System.Runtime.InteropServices.JavaScript;

namespace SqliteWasmBlazor.TestApp.TestInfrastructure;

/// <summary>
/// Which worker bundle this run boots. Read from the page URL before DI is
/// configured, because the choice is a registration: with
/// <c>AddSqliteWasmBlazorCrypto</c> the bridge loads the Crypto bundle, and
/// nothing else can point it back.
/// </summary>
internal static partial class TestPlane
{
    private const string ModuleName = "testPlane";

    /// <summary>
    /// <c>true</c> when the URL asked for the plain bundle. Call
    /// <see cref="ResolveAsync"/> first.
    /// </summary>
    public static bool IsPlain { get; private set; }

    /// <summary>
    /// Import the module and read the plane. Runs in <c>Program.cs</c> before
    /// <c>builder.Build()</c> — the WASM runtime is already up by then, so
    /// JS interop is available.
    /// </summary>
    public static async Task ResolveAsync(string baseHref)
    {
        await JSHost.ImportAsync(ModuleName, $"{baseHref}test-plane.js");
        IsPlain = string.Equals(Plane(), "plain", StringComparison.Ordinal);
    }

    [JSImport("plane", ModuleName)]
    private static partial string Plane();
}
