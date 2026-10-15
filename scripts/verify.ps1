param(
    [string]$Toolchain = '',
    [switch]$Offline
)
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$cargoPrefix = @()
if ($Toolchain) { $cargoPrefix = @("+$Toolchain") }
$networkArgs = @()
if ($Offline) { $networkArgs = @('--offline') }
Push-Location -LiteralPath $repoRoot
try {
    # Formatting uses the installed default rustfmt, even if a linker toolchain is selected.
    & cargo fmt --all --check
    if ($LASTEXITCODE -ne 0) { throw 'Formatting failed' }
    & cargo @cargoPrefix test --workspace --all-targets --locked @networkArgs
    if ($LASTEXITCODE -ne 0) { throw 'Workspace tests failed' }
    & cargo @cargoPrefix test --workspace --doc --locked @networkArgs
    if ($LASTEXITCODE -ne 0) { throw 'Documentation tests failed' }
    & cargo @cargoPrefix clippy --workspace --all-targets --locked @networkArgs -- -D warnings
    if ($LASTEXITCODE -ne 0) { throw 'Clippy failed' }
} finally {
    Pop-Location
}
