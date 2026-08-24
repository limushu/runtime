$ErrorActionPreference = "Stop"

$toolchain = "stable-x86_64-pc-windows-gnu"
$rustupRoot = if ($env:RUSTUP_HOME) {
    $env:RUSTUP_HOME
} else {
    Join-Path $env:USERPROFILE ".rustup"
}

$needsAsciiWorkaround = $rustupRoot -match "[^\x00-\x7F]"
$mappedDrive = $null
$oldRustFlags = $env:RUSTFLAGS

try {
    if ($needsAsciiWorkaround) {
        foreach ($letter in @("R", "Q", "P", "O")) {
            if (-not (Get-PSDrive -Name $letter -ErrorAction SilentlyContinue)) {
                $mappedDrive = "${letter}:"
                subst $mappedDrive $rustupRoot
                break
            }
        }
        if (-not $mappedDrive) {
            throw "No free drive letter is available for the MinGW ASCII-path workaround."
        }
        $asciiSysroot = "$mappedDrive\toolchains\$toolchain"
        $env:RUSTFLAGS = if ($oldRustFlags) {
            "$oldRustFlags --sysroot=$asciiSysroot"
        } else {
            "--sysroot=$asciiSysroot"
        }
    }

    cargo fmt --all -- --check
    cargo "+$toolchain" check --all-targets
    cargo "+$toolchain" test --all-targets
    cargo "+$toolchain" run --example rebuild
}
finally {
    $env:RUSTFLAGS = $oldRustFlags
    if ($mappedDrive) {
        subst $mappedDrive /d
    }
}
