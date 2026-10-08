"""Record and verify the exact cross-built test archive consumed by CI."""

import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import sys

mode, target, stage = sys.argv[1:]
archive = Path("windows-tests.tar.zst")
identity_path = Path("build-identity.json")
digest = hashlib.sha256()
with archive.open("rb") as source:
    while chunk := source.read(1024 * 1024):
        digest.update(chunk)
expected = {
    "source_revision": os.environ["GITHUB_SHA"],
    "target": target,
    "stage": stage,
    "profile": "test (inherits dev)",
    "workspace_opt_level": 0,
    "features": ["jj-cli/test-fakes"],
    "archive_sha256": digest.hexdigest(),
}
if mode == "write":
    binary = Path("target") / target / "debug" / "jj.exe"
    with binary.open("rb") as executable:
        executable.seek(0x3C)
        offset = struct.unpack("<I", executable.read(4))[0]
        executable.seek(offset)
        signature, machine = struct.unpack("<4sH", executable.read(6))
    expected_machine = {"x86_64-pc-windows-msvc": 0x8664, "aarch64-pc-windows-msvc": 0xAA64}[target]
    if signature != b"PE\0\0" or machine != expected_machine:
        raise SystemExit(f"Unexpected executable architecture: {signature!r}, {machine:#x}")
    with binary.open("rb") as executable:
        executable.seek(offset + 24 + 72)
        stack_reserve = struct.unpack("<Q", executable.read(8))[0]
    if stack_reserve != 8388608:
        raise SystemExit(f"Expected native 8 MiB stack, got {stack_reserve}")
    expected["stack_reserve"] = stack_reserve
    expected["effective_rustflags"] = os.environ["CARGO_ENCODED_RUSTFLAGS"].split("\x1f")
    expected["binary_machine"] = hex(machine)
    expected["rustc"] = subprocess.check_output(["rustc", "-vV"], text=True)
    expected["cross_compiler"] = "cargo-xwin 0.23.1 / clang-cl / MSVC"
    expected["command"] = (
        "cargo nextest archive --config .cargo/config-ci.toml --workspace "
        "--all-targets --features jj-cli/test-fakes --target " + target
        + " --archive-file windows-tests.tar.zst"
    )
    identity_path.write_text(json.dumps(expected, indent=2) + "\n")
elif mode == "verify":
    identity = json.loads(identity_path.read_text())
    for key, value in expected.items():
        if identity.get(key) != value:
            raise SystemExit(f"Archive identity mismatch: {key}: {identity.get(key)!r} != {value!r}")
else:
    raise SystemExit(f"Unknown mode: {mode}")
print(identity_path.read_text())
