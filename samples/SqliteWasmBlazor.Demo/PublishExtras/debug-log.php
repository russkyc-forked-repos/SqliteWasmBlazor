<?php
// Lightweight debug-log sink for the Blazor demo. The worker + bridge POST
// JSON phase events to this file; we append them to debug-log.txt next to
// it. If this file isn't deployed the POSTs 404 silently — the client
// catch swallows the error and the operation continues unaffected.
//
// Lives under PublishExtras/ (outside wwwroot) so a regular `dotnet
// publish` does not include it. scripts/deep-clean-publish.sh copies it
// into $PUB/debug-log.php when invoked, so the diagnostic endpoint only
// ships from the explicit deploy recipe.
//
// Self-rotates at 4 MB so a forgotten log can't fill the host. Each entry
// is one line: `<server-iso-timestamp> <client-json>`.

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    exit;
}

$logPath = __DIR__ . '/debug-log.txt';
$maxBytes = 4 * 1024 * 1024;

if (is_file($logPath) && filesize($logPath) > $maxBytes) {
    @rename($logPath, $logPath . '.prev');
}

$body = file_get_contents('php://input');
if ($body === false || $body === '') {
    http_response_code(204);
    exit;
}

$stamp = date('Y-m-d\TH:i:s.v\Z');
$line = $stamp . ' ' . str_replace(["\r", "\n"], ' ', $body) . "\n";

@file_put_contents($logPath, $line, FILE_APPEND | LOCK_EX);
http_response_code(204);
