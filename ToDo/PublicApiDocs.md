# Public API documentation

Consumers of these packages get IntelliSense from the XML doc that ships in
each `.nupkg`. Until recently none shipped at all — `GenerateDocumentationFile`
was set nowhere, so a heavily-documented API arrived as a bare `.dll`. That is
fixed; what remains is filling the gaps the fix made visible.

## Where it stands

| Package | Undocumented public members | Gate |
| --- | --- | --- |
| `SqliteWasmBlazor` | **0** | **CS1591 is an error** — a new undocumented public member fails the build |
| `SqliteWasmBlazor.Crypto` | 19 | CS1591 suppressed |
| `SqliteWasmBlazor.Crypto.UI` | 57 | CS1591 suppressed |

CS1591 is suppressed solution-wide in `Directory.Build.props`. The base library
opts back in from its own `.csproj` and turns it into an error. **Each package
does the same as it reaches zero** — that is what stops the count drifting back
up, and it is a two-line change:

```xml
<NoWarn>$(NoWarn.Replace('CS1591',''))</NoWarn>
<WarningsAsErrors>$(WarningsAsErrors);CS1591</WarningsAsErrors>
```

## Follow-up: `SqliteWasmBlazor.Crypto` — 19

Not yet surveyed member by member. Do this one first: it is the smaller of the
two and it is real API surface rather than component plumbing, so it sets the
voice for `Crypto.UI`.

## Follow-up: `SqliteWasmBlazor.Crypto.UI` — 57

Surveyed. Almost all of it must stay public, and the reasons are worth knowing
before starting:

- **`EncryptionModel` (18) and `AuthenticationModel` (12)** — the demo drives
  these from `.razor`. `AuthenticationModel.ClearKeysAsync` has an external
  caller in `DemoSessionAuthenticator`.
- **Model constructors (5)** — `public partial` is the shape RxBlazorV2's
  generator requires. Not a choice.
- **`OnContextReady` / `OnContextReadyAsync` (3)** — `protected override`.
  CS1591 covers `protected` too, since a consumer deriving from the type sees
  those members. They cannot be hidden, only documented.
- **`DatabaseErrorAlertModel` (4)** — has no external reference, but cannot go
  internal: the public `DatabaseErrorAlert` component exposes a `Model`
  property of that type, and CS0050 forbids the inconsistency. The same
  constraint binds every RxBlazor model in this package.
- **The four `[Inject]` properties** on `PublicKeyDisplay` and
  `DatabaseErrorAlert` — `public required`. Making them private would close a
  real leak (a consumer can currently see and set a component's DI wiring) but
  CS9032 forbids a `required` member less visible than its type, so it would
  mean trading `required` for `= null!`. The null-forgiving operator is what
  `required` exists to avoid. They stay public and get documented.

## What is already done, so it is not redone

- **109 `<inheritdoc />`** on ADO.NET overrides — `DbDataReader`, `DbCommand`,
  `DbParameter`, `DbConnection`, `DbTransaction` — plus the eight
  `IDbInitFailure.DefaultMessage` implementations. The contract belongs to the
  base type; restating it would only let the copies drift.
- **2 members internalised** — `AuthenticationModel.SignOutAsync` and
  `DismissWrongPasskey` had no caller outside their assembly.
- **34 broken doc references fixed.** Over half were resolvable and merely
  written wrong: `AssetRoot` / `BaseHref` are inherited from
  `SqliteWasmAssetOptions` and a cref through the derived options type cannot
  bind; `NotifyAuthenticationStateChanged` is protected on the base type and a
  class-level doc comment resolves in namespace scope, not class scope. The
  rest named types that do not exist anywhere (`IErrorModel`,
  `SigningService`) or cannot be referenced across plane boundaries.

## Watch out for

- `dotnet build` will not re-emit these warnings on an up-to-date build. Use
  `--no-incremental` or you will read a stale silence as success.
- `tools/XmlDocPruner` strips non-public entries from the shipped XML, so a
  `///` comment on an internal type still helps the team in-solution without
  reaching consumers. Documenting an internal member is therefore never
  required — only useful.
