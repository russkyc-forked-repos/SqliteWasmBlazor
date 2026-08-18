using Microsoft.Playwright;
using SqliteWasmBlazor.Tests.Infrastructure;
using Xunit.Abstractions;

namespace SqliteWasmBlazor.Tests;

[CollectionDefinition("PrfWebAuthn", DisableParallelization = true)]
public class PrfWebAuthnCollection : ICollectionFixture<PrfWaFixture>
{
}

[Collection("PrfWebAuthn")]
public class PrfVirtualAuthenticatorTests(PrfWaFixture fixture, ITestOutputHelper output)
{
    private const string PrfTestPath = "/prf-vfs-test";

    // Dev-friendly waits: keep the first-button-visible wait long because it
    // covers a cold WASM boot, but compress everything afterwards so a stuck
    // step trips quickly during iteration.
    private const float FirstButtonVisibleTimeoutMs = 60000;
    private const float ButtonEnabledTimeoutMs = 10000;
    private const float StatusTimeoutMs = 8000;
    // Post-TTL button re-enable budget. After SessionTtlMs the JS-side key
    // cache clears, KeyExpired observable fires, fans out to UI, and Blazor
    // re-renders the AuthorizeView. That cascade is slower than a normal
    // button-enable on a fresh page load — CI runners need ~10–15 s, give
    // 20 s headroom.
    private const float KeyExpiredButtonEnableTimeoutMs = 20000;

    // Mirrors KeyCacheOptions.TtlMs configured in TestApp.Program.cs. Tests
    // that drive session expiry must wait > this, then receive "PRF session
    // ended" within StatusTimeoutMs.
    private const int SessionTtlMs = 5000;

    [Fact]
    [Trait("Browser", "Chromium")]
    public async Task RegistrationHappyPath_StoresCredential()
    {
        await using var scenario = await fixture.CreateScenarioAsync(output);

        await scenario.NavigateAsync(PrfTestPath);

        // First click absorbs the cold WASM boot; subsequent clicks fall back
        // to the dev-friendly ButtonEnabledTimeoutMs.
        await ClickAsync(scenario, "Register passkey", FirstButtonVisibleTimeoutMs);
        await ExpectStatusContainsAsync(scenario, "Passkey registered");

        var credentials = await GetCredentialsAsync(scenario, scenario.PrimaryAuthenticatorId);
        Assert.True(credentials.GetArrayLength() == 1,
            $"Expected 1 credential after registration, found {credentials.GetArrayLength()}.");

        var credentialId = credentials[0].GetProperty("credentialId").GetString();
        Assert.False(string.IsNullOrEmpty(credentialId), "Virtual authenticator returned an empty credential id.");
    }

    [Fact]
    [Trait("Browser", "Chromium")]
    public async Task CachedCredential_RoundTripsThroughEncryptedVfs()
    {
        await using var scenario = await fixture.CreateScenarioAsync(output);

        await scenario.NavigateAsync(PrfTestPath);

        // First click absorbs the cold WASM boot; subsequent clicks fall back
        // to the dev-friendly ButtonEnabledTimeoutMs.
        await ClickAsync(scenario, "Register passkey", FirstButtonVisibleTimeoutMs);
        await ExpectStatusContainsAsync(scenario, "Passkey registered");

        await ClickAsync(scenario, "Authenticate and open");
        // Current UI separates authentication/key install from the first
        // write; Insert materialises the encrypted DB after auth succeeds.
        await ExpectStatusContainsAsync(scenario, "Authenticated.");

        await ClickAsync(scenario, "Insert + read 25 rows");
        await ExpectStatusContainsAsync(scenario, "Round trip OK — total rows: 25");

        await ClickAsync(scenario, "Read row count (no writes)");
        await ExpectStatusContainsAsync(scenario, "Row count: 25");
    }

    [Fact]
    [Trait("Browser", "Chromium")]
    public async Task CredentialMismatch_SurfacesWrongKey()
    {
        await using var scenario = await fixture.CreateScenarioAsync(output);

        await scenario.NavigateAsync(PrfTestPath);

        // Stage 1 — Register A → boot a fresh DB encrypted under A's pubkey-bytes.
        await ClickAsync(scenario, "Register passkey", FirstButtonVisibleTimeoutMs);
        await ExpectStatusContainsAsync(scenario, "Passkey registered");

        // Capture A's credentialId before B exists so we can target it for
        // removal in Stage 2.
        var afterA = await GetCredentialsAsync(scenario, scenario.PrimaryAuthenticatorId);
        Assert.True(afterA.GetArrayLength() == 1,
            $"Expected 1 credential after registering A, found {afterA.GetArrayLength()}.");
        var credentialA = afterA[0].GetProperty("credentialId").GetString()
            ?? throw new InvalidOperationException("Credential A returned a null credentialId.");

        await ClickAsync(scenario, "Authenticate and open");
        await ExpectStatusContainsAsync(scenario, "Authenticated.");
        await ClickAsync(scenario, "Insert + read 25 rows");
        await ExpectStatusContainsAsync(scenario, "Round trip OK — total rows: 25");
        await ClickAsync(scenario, "Lock (close DB + drop PRF session)");
        await ExpectStatusContainsAsync(scenario, "DB closed");

        // Stage 2 — Register a second credential B on the SAME authenticator.
        // Matches the page's documented "register a second passkey" workflow.
        // Pick A's credential off the authenticator afterwards so the
        // discoverable PRF ceremony has only B to answer with — avoids
        // depending on Chrome's auto-presence credential ordering, which is
        // undefined when multiple resident credentials live on one virtual
        // authenticator.
        await ClickAsync(scenario, "Register passkey");
        await ExpectStatusContainsAsync(scenario, "Passkey registered");

        var bothCreds = await GetCredentialsAsync(scenario, scenario.PrimaryAuthenticatorId);
        Assert.True(bothCreds.GetArrayLength() == 2,
            $"Expected 2 credentials after registering B, found {bothCreds.GetArrayLength()}.");

        await scenario.SendCdpAsync("WebAuthn.removeCredential", new Dictionary<string, object>
        {
            ["authenticatorId"] = scenario.PrimaryAuthenticatorId,
            ["credentialId"] = credentialA,
        });

        // Stage 3 — only B can answer; the disk's manifest is owned by A.
        // The page's wrong-passkey early-out reads the manifest credentialId
        // via Session.GetStateAsync() and refuses BEFORE installing the
        // wrong-fit globalKey, so the failure surfaces at the auth step
        // (clean message) instead of as SQLITE_IOERR on the first read.
        await ClickAsync(scenario, "Authenticate and open");
        await ExpectStatusContainsAsync(scenario, "Wrong passkey for this DB");
    }

    [Fact]
    [Trait("Browser", "Chromium")]
    public async Task SessionExpiresOnTtl_DropsKeyAndReEnablesAuth()
    {
        await using var scenario = await fixture.CreateScenarioAsync(output);

        await scenario.NavigateAsync(PrfTestPath);

        // First click absorbs the cold WASM boot; subsequent clicks fall back
        // to the dev-friendly ButtonEnabledTimeoutMs.
        await ClickAsync(scenario, "Register passkey", FirstButtonVisibleTimeoutMs);
        await ExpectStatusContainsAsync(scenario, "Passkey registered");

        await ClickAsync(scenario, "Authenticate and open");
        await ExpectStatusContainsAsync(scenario, "Authenticated.");

        // Authenticate buttons stay disabled while a PRF session is active
        // (Disabled bindings on PrfService.HasCachedKeys()). Confirm the
        // pre-expiry state before the timer fires.
        var authButton = scenario.Page.GetByRole(AriaRole.Button,
            new() { Name = "Authenticate and open", Exact = true });
        await Assertions.Expect(authButton).ToBeDisabledAsync(
            new() { Timeout = ButtonEnabledTimeoutMs });

        // Wait past the configured TTL. SecureKeyCache + JS-side key cache
        // both fire after SessionTtlMs; KeyExpired observable fans out to
        // PrfVfsTest.OnKeyExpired which clears _keyInstalled / _armoredOwnPubkey
        // and posts the "PRF session ended" alert.
        await ExpectStatusContainsAsync(
            scenario,
            "PRF session ended",
            timeoutMs: SessionTtlMs + StatusTimeoutMs);

        // After the timer fires HasCachedKeys() returns false, so the
        // Authenticate button must re-enable — the timer/observable wire-up
        // is the path under test (Lock + KeyExpired-fires-UI-update is
        // already covered by scenario 3). Uses the dedicated
        // KeyExpiredButtonEnableTimeoutMs because the JS→C# observable +
        // Blazor re-render cascade is slower than a fresh-page button-enable
        // — CI runners hit the original 8s StatusTimeoutMs ceiling here.
        await Assertions.Expect(authButton).ToBeEnabledAsync(
            new() { Timeout = KeyExpiredButtonEnableTimeoutMs });
    }

    [Fact]
    [Trait("Browser", "Chromium")]
    public async Task ExcludedCredential_RefusesDuplicateRegistration()
    {
        await using var scenario = await fixture.CreateScenarioAsync(output);

        await scenario.NavigateAsync(PrfTestPath);

        // First click absorbs the cold WASM boot; subsequent clicks fall back
        // to the dev-friendly ButtonEnabledTimeoutMs.
        await ClickAsync(scenario, "Register passkey", FirstButtonVisibleTimeoutMs);
        await ExpectStatusContainsAsync(scenario, "Passkey registered");

        var afterFirst = await GetCredentialsAsync(scenario, scenario.PrimaryAuthenticatorId);
        Assert.True(afterFirst.GetArrayLength() == 1,
            $"Expected 1 credential after registration, found {afterFirst.GetArrayLength()}.");

        // Re-register naming the existing credential in excludeCredentials. The
        // authenticator already holds it, so WebAuthn raises InvalidStateError,
        // which webauthn.ts maps to CREDENTIAL_ALREADY_REGISTERED. Asserting the
        // literal token also pins the JS -> C# enum wire format: PrfJsonContext
        // matches C# member names verbatim, so a PascalCase value would throw
        // JsonException instead of arriving as a structured failure.
        await ClickAsync(scenario, "Register passkey (exclude current)");
        await ExpectStatusContainsAsync(scenario, "CREDENTIAL_ALREADY_REGISTERED");

        // The browser's own DOMException text must reach the UI. Discarding it is
        // what made a rejected security-key PIN indistinguishable from a dismissed
        // prompt — both arrive as NotAllowedError with only the message differing.
        await ExpectStatusContainsAsync(scenario, "InvalidStateError");

        // The whole point: no duplicate passkey was minted.
        var afterSecond = await GetCredentialsAsync(scenario, scenario.PrimaryAuthenticatorId);
        Assert.True(afterSecond.GetArrayLength() == 1,
            $"excludeCredentials was ignored — found {afterSecond.GetArrayLength()} credentials, expected 1.");
    }

    [Fact]
    [Trait("Browser", "Chromium")]
    public async Task CrossPlatformAuthenticator_RegistersAndRoundTrips()
    {
        await using var scenario = await fixture.CreateScenarioAsync(output);

        // Drop the platform authenticator so a USB security key is the only thing
        // that can answer. This whole scenario was unreachable while registration
        // pinned authenticatorAttachment to "platform": create() excluded
        // cross-platform authenticators outright, so a security key could never be
        // enrolled even though the assertion path would happily have used one.
        await scenario.RemoveAuthenticatorAsync(scenario.PrimaryAuthenticatorId);
        await scenario.AddVirtualAuthenticatorAsync(transport: "usb");

        await scenario.NavigateAsync(PrfTestPath);

        // Register runs create() then an immediate PRF assertion. The assertion
        // carries the transports getTransports() reported on the attestation, so it
        // targets the key that just answered instead of walking every transport.
        await ClickAsync(scenario, "Register passkey", FirstButtonVisibleTimeoutMs);
        await ExpectStatusContainsAsync(scenario, "Passkey registered");

        await ClickAsync(scenario, "Authenticate and open");
        await ExpectStatusContainsAsync(scenario, "Authenticated.");

        await ClickAsync(scenario, "Insert + read 25 rows");
        await ExpectStatusContainsAsync(scenario, "Round trip OK — total rows: 25");
    }

    [Theory]
    [InlineData("internal")]
    [InlineData("usb")]
    [Trait("Browser", "Chromium")]
    public async Task RegisterThenDerive_Completes(string transport)
    {
        await using var scenario = await fixture.CreateScenarioAsync(output);

        if (transport != "internal")
        {
            await scenario.RemoveAuthenticatorAsync(scenario.PrimaryAuthenticatorId);
            await scenario.AddVirtualAuthenticatorAsync(transport);
        }

        await scenario.NavigateAsync(PrfTestPath);

        // The Crypto.UI panel's register path is create() + assertion + id check
        // (PrfAuthenticator.RegisterAsync). Every other test here stops after
        // create() or derives without having just registered, so this is the only
        // cover for the two-ceremony sequence the panel actually runs.
        await ClickAsync(scenario, "Register + derive", FirstButtonVisibleTimeoutMs);
        await ExpectStatusContainsAsync(scenario, "Register + derive OK");
    }

    private static async Task ClickAsync(PrfScenario scenario, string buttonName, float? timeoutMs = null)
    {
        var button = scenario.Page.GetByRole(AriaRole.Button, new() { Name = buttonName, Exact = true });
        await Assertions.Expect(button).ToBeEnabledAsync(new() { Timeout = timeoutMs ?? ButtonEnabledTimeoutMs });
        await button.ClickAsync();
    }

    private static Task ExpectStatusContainsAsync(PrfScenario scenario, string substring, float? timeoutMs = null)
    {
        var alert = scenario.Page.Locator(".mud-alert").Last;
        return Assertions.Expect(alert).ToContainTextAsync(substring, new() { Timeout = timeoutMs ?? StatusTimeoutMs });
    }

    private static async Task<System.Text.Json.JsonElement> GetCredentialsAsync(PrfScenario scenario, string authenticatorId)
    {
        var response = await scenario.SendCdpAsync(
            "WebAuthn.getCredentials",
            new Dictionary<string, object>
            {
                ["authenticatorId"] = authenticatorId,
            });
        Assert.NotNull(response);
        return response.Value.GetProperty("credentials");
    }
}
