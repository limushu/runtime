#!/usr/bin/env python3
"""Reject Cargo dependencies that violate the control-plane package boundaries."""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class PackageLocation:
    area: str


def locate(manifest_path: str) -> PackageLocation:
    relative = Path(manifest_path).resolve().relative_to(ROOT).as_posix()
    parts = relative.split("/")
    if parts[:2] == ["crates", "foundation"]:
        return PackageLocation("foundation")
    if parts[:2] == ["crates", "pool-control-plane"]:
        return PackageLocation("pool")
    return PackageLocation("other")


def cargo_metadata() -> dict:
    command = [
        "cargo",
        "metadata",
        "--format-version",
        "1",
        "--no-deps",
        "--manifest-path",
        str(ROOT / "Cargo.toml"),
    ]
    completed = subprocess.run(command, check=True, capture_output=True, text=True)
    return json.loads(completed.stdout)


def verify_source_shape(violations: list[str]) -> None:
    runtime = ROOT / "crates" / "foundation" / "control-runtime" / "src"
    pool = ROOT / "crates" / "pool-control-plane" / "src"

    forbidden_paths = [runtime / "router.rs", pool / "pool_runtime.rs"]
    for path in forbidden_paths:
        if path.exists():
            violations.append(f"obsolete architecture file must not exist: {path.relative_to(ROOT)}")

    required_paths = [
        runtime / "client.rs",
        pool / "pool_manager.rs",
        pool / "pool" / "instance.rs",
        pool / "pool" / "model.rs",
        pool / "domains" / "member_disk" / "model.rs",
        pool / "domains" / "member_disk" / "actor.rs",
    ]
    for path in required_paths:
        if not path.is_file():
            violations.append(f"required architecture file is missing: {path.relative_to(ROOT)}")

    protocol = (runtime / "protocol.rs").read_text(encoding="utf-8")
    service_request = protocol.split("pub trait ServiceRequest", 1)[-1]
    if "fn service_id" in service_request:
        violations.append("ServiceRequest must not carry hidden service routing identity")

    service_loop = (runtime / "service" / "service_loop.rs").read_text(encoding="utf-8")
    if "HashMap<ObjectKey, ObjectSlot" not in service_loop:
        violations.append("ServiceLoop must use ObjectSlot as its managed-intent index")

    foundation_terms = ("MemberDisk", "VirtualDisk", "PoolNode", "BlkId", "PhysicalDiskId")
    for source in runtime.rglob("*.rs"):
        text = source.read_text(encoding="utf-8")
        for term in foundation_terms:
            if term in text:
                violations.append(
                    f"foundation contains Pool business term {term}: {source.relative_to(ROOT)}"
                )


def main() -> int:
    metadata = cargo_metadata()
    packages = {package["name"]: package for package in metadata["packages"]}
    locations = {
        name: locate(package["manifest_path"]) for name, package in packages.items()
    }
    violations: list[str] = []

    for source_name, package in packages.items():
        source = locations[source_name]
        for dependency in package["dependencies"]:
            target_name = dependency["name"]
            target = locations.get(target_name)
            if target is None:
                continue

            edge = f"{source_name} -> {target_name}"
            if source.area == "foundation" and target.area != "foundation":
                violations.append(f"foundation may only depend on foundation: {edge}")
    domain_root = ROOT / "crates" / "pool-control-plane" / "src" / "domains"
    if not domain_root.is_dir():
        violations.append("pool-control-plane must contain src/domains")
    for manifest in domain_root.rglob("Cargo.toml") if domain_root.exists() else []:
        violations.append(
            f"a Domain is a Rust module, not a Cargo package: {manifest.relative_to(ROOT)}"
        )

    verify_source_shape(violations)

    if violations:
        print("architecture dependency check failed:", file=sys.stderr)
        for violation in violations:
            print(f"  - {violation}", file=sys.stderr)
        return 1

    print(f"architecture dependency check passed for {len(packages)} workspace packages")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
