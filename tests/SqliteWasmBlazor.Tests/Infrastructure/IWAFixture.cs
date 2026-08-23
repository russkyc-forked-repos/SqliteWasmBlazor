using Microsoft.Playwright;

namespace SqliteWasmBlazor.Tests.Infrastructure;

public interface IWaFixture
{
    public enum BrowserType
    {
        NONE = 0,
        CHROMIUM = 1,
        FIREFOX = 2,
        WEBKIT = 4,
        ALL = 7
    }

    public Task InitializeAsync();
    public IPage? Page { get; }
    public BrowserType Type { get; }
    public int Port { get; }
    public bool OnePass { get; }
    public bool Headless { get; }

    /// <summary>
    /// Query appended to every navigation, including the empty string. The
    /// TestApp reads <c>?plane=plain</c> here to boot the plain worker bundle
    /// instead of the Crypto one.
    /// </summary>
    public string Query => string.Empty;
}
