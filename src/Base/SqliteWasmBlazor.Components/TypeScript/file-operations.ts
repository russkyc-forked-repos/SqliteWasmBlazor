// file-operations.ts
// Components asset-module bootstrap, loaded by FileOperationsInterop.InitializeAsync.
// Upload is handled by Blazor's InputFile component; database downloads go
// through the worker bridge's staged export path
// (ISqliteWasmDatabaseService.ExportDatabaseToDownloadAsync), which never
// routes bytes through managed or main-thread memory.

export {};
