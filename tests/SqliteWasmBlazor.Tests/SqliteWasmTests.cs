using SqliteWasmBlazor.TestApp.TestInfrastructure;
using SqliteWasmBlazor.Tests.Infrastructure;

namespace SqliteWasmBlazor.Tests;

[CollectionDefinition("Chromium", DisableParallelization = true)]
public class ChromiumCollection : ICollectionFixture<ChromiumFixture>
{
    // This class has no code, and is never created. Its purpose is simply
    // to be the place to apply [CollectionDefinition] and all the
    // ICollectionFixture<> interfaces.
}

[Collection("Chromium")]
public class ChromiumTest(ChromiumFixture fixture, Xunit.Abstractions.ITestOutputHelper output)
    : SqliteWasmTestBase(fixture, output)
{
    [Theory]
    [MemberData(nameof(TestRegistry.NamesAsTheoryData), MemberType = typeof(TestRegistry))]
    public Task TestCaseAsync(string name) => RunCaseAsync(name);
}

[CollectionDefinition("PlainPlane", DisableParallelization = true)]
public class PlainPlaneCollection : ICollectionFixture<PlainPlaneFixture>
{
    // Marker class — no code needed.
}

/// <summary>
/// The same tests, run against the <b>plain</b> worker bundle.
///
/// <para>
/// The TestApp references SqliteWasmBlazor.Crypto, so every other fixture
/// boots the Crypto bundle — which means base's own worker cases
/// (<c>replaceDb</c>, the four import-session ops, the streaming
/// export/import handlers, the init park sweep) were shipped but never
/// executed by anything in the repo. This fixture boots the app with
/// <c>?plane=plain</c>, which leaves the Crypto services unregistered and the
/// bridge pointed at the base bundle.
/// </para>
///
/// <para>
/// The name list is <see cref="TestRegistry.PlainPlaneNames"/> — everything
/// that needs nothing beyond <c>AddSqliteWasm</c>. TestFactory asserts both
/// directions of parity against it on this plane, so a test that drifts out
/// of the list fails at page construction rather than silently going
/// unexercised.
/// </para>
/// </summary>
[Collection("PlainPlane")]
public class PlainPlaneTest(PlainPlaneFixture fixture, Xunit.Abstractions.ITestOutputHelper output)
    : SqliteWasmTestBase(fixture, output)
{
    [Theory]
    [MemberData(nameof(TestRegistry.PlainNamesAsTheoryData), MemberType = typeof(TestRegistry))]
    public Task PlainPlaneCaseAsync(string name) => RunCaseAsync(name);
}

// Firefox and WebKit tests disabled due to Playwright compatibility issues
// Firefox: Working in browser but disabled for now
// WebKit: Out of memory errors in Playwright (works fine in Safari)
#if NEVER_DEFINED
[CollectionDefinition("Firefox", DisableParallelization = true)]
public class FirefoxCollection : ICollectionFixture<FirefoxFixture>
{
    // This class has no code, and is never created. Its purpose is simply
    // to be the place to apply [CollectionDefinition] and all the
    // ICollectionFixture<> interfaces.
}

[Collection("Firefox")]
public class FirefoxTest(FirefoxFixture fixture) : SqliteWasmTestBase(fixture)
{
}

#if !Windows
[CollectionDefinition("Webkit", DisableParallelization = true)]
public class WebkitCollection : ICollectionFixture<WebkitFixture>
{
    // This class has no code, and is never created. Its purpose is simply
    // to be the place to apply [CollectionDefinition] and all the
    // ICollectionFixture<> interfaces.
}

[Collection("Webkit")]
public class WebkitTest(WebkitFixture fixture) : SqliteWasmTestBase(fixture)
{
}
#endif
#endif
