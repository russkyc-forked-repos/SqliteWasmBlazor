namespace SqliteWasmBlazor.TestApp.TestInfrastructure;

/// <summary>
/// Single source of truth for test-case names. Both <see cref="TestFactory"/>
/// (the TestApp Blazor dispatcher) and <c>SqliteWasmTestBase</c> (the
/// Playwright host xUnit Theory) consume this list. Adding or removing a
/// test means updating this list and the matching <c>Add(...)</c> line in
/// <see cref="TestFactory"/>; drift is asserted at TestFactory construction.
/// </summary>
public static class TestRegistry
{
    public static readonly IReadOnlyList<string> AllNames =
    [
        // Type Marshalling
        "AllTypes_RoundTrip",
        "IntegerTypes_Boundaries",
        "NullableTypes_AllNull",
        "BinaryData_LargeBlob",
        "StringValue_Unicode",
        "DateTimeOffset_TextStorage",
        "TimeSpan_Conversion",
        "Char_SingleCharString",
        "Guid_Utf8ByteArray",
        "Guid_HasDataSeedQuery",

        // JSON Collections
        "IntList_RoundTrip",
        "IntList_Empty",
        "IntList_LargeCollection",

        // CRUD
        "Create_SingleEntity",
        "Read_ById",
        "UpdateModifyProperty",
        "Delete_SingleEntity",
        "BulkInsert_100Entities",
        "FTS5_Search",
        "FTS5_SoftDeleteThenClear",

        // Transactions
        "Transaction_Commit",
        "Transaction_Rollback",

        // Relationships
        "TodoList_CreateWithGuidKey",
        "Todo_CreateWithForeignKey",
        "TodoList_IncludeNavigation",
        "TodoList_CascadeDelete",
        "Todo_ComplexQueryWithJoin",
        "Todo_NullableDateTime",

        // Migrations
        "Migration_FreshDatabaseMigrate",
        "Migration_ExistingDatabaseIdempotent",
        "Migration_HistoryTableTracking",
        "Migration_GetAppliedMigrations",
        "Migration_DatabaseExistsCheck",
        "Migration_EnsureCreatedVsMigrateConflict",
        "MigrationRecovery_HistoryRebuildSucceeds",
        "MigrationRecovery_DroppedColumnSurfacesMismatch",
        "MigrationRecovery_ExtraColumnSurfacesMismatch",

        // Race Conditions
        "RaceCondition_PurgeThenLoad",
        "RaceCondition_PurgeThenLoadWithTransaction",

        // EF Core Functions
        "EFCoreFunctions_DecimalArithmetic",
        "EFCoreFunctions_DecimalAggregates",
        "EFCoreFunctions_DecimalComparison",
        "EFCoreFunctions_DecimalComparisonSimple",
        "EFCoreFunctions_RegexPattern",
        "EFCoreFunctions_ComplexDecimalQuery",
        "EFCoreFunctions_AggregateBuiltIn",

        // Raw Database Import/Export
        "ExportImport_RawDatabase",
        "ImportRawDatabase_InvalidFile",
        "ImportRawDatabase_WithBackup",
        "ImportRawDatabase_BackupRestoreOnFailure",
        "ExportRawDatabase_ReOpenAfterExport",
        "ExportRawDatabase_StagedDownload",
        "ImportRawDatabase_IntoNewDatabase",
        "ImportRawDatabase_IncompatibleSchema",
        "ImportRawDatabase_AutoReOpenAfterImport",
        "ImportRawDatabase_SequentialImports",
        "ImportExportRawDatabase_ImportThenExport",
        "ImportRawDatabase_SchemaValidationExtension",

        // Checkpoints
        "RestoreToCheckpoint_Basic",
        "RestoreToCheckpoint_WithDeltaReapply",

        // VFS Encryption
        "VFS_EncryptedRoundTrip",
        "VFS_OnDiskCiphertext",
        "VFS_PlainRegression",
        "VFS_WrongKeyFails",
        "VFS_TamperDetection",
        "VFS_ModeMismatch",
        "VFS_PhysicalLayout",
        "VFS_PerformanceSmoke",
        "VFS_PerformanceSmoke_SameJournalMode",
        "VFS_ManifestMacRejectsWrongKey",
        "VFS_EnterEncrypted_PreExistingPlainDb",

        // VFS Encryption — PRF synthetic-seed compositions
        "PRF_CredentialMismatchSurfacesTypedFailure",
        "Synthetic_PrfSeed_DrivesEncryptedVfsRoundTrip",
        "Synthetic_PrfSeed_EncryptInPlace_PreservesRowsUnderKey",
        "Synthetic_PrfSeed_DecryptInPlace_PreservesRowsAsPlain",
        "SingleDb_StreamingImport_PlainAndEncryptedRoundTrip",
        "Plain_DbsEnvelope_StreamingImport_RoundTrip",
        "DiskImport_GuidedCrossKey_StreamingRoundTrip",
    ];

    /// <summary>
    /// xUnit <c>[MemberData]</c> adapter for the names list. Each row is a
    /// single-element <c>object[]</c> consumed by the Playwright <c>[Theory]</c>
    /// in <c>SqliteWasmTestBase</c>.
    /// </summary>
    public static IEnumerable<object[]> NamesAsTheoryData =>
        AllNames.Select(name => new object[] { name });
}
