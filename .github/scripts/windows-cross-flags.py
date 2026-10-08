"""Preserve SDK flags while adding the repository's native MSVC flags."""
import os
from pathlib import Path
import shlex
import tomllib

encoded = os.environ.get("CARGO_ENCODED_RUSTFLAGS")
if encoded is not None:
    flags = encoded.split("\x1f") if encoded else []
else:
    target_key = "CARGO_TARGET_" + os.environ["TARGET"].upper().replace("-", "_") + "_RUSTFLAGS"
    flags = shlex.split(os.environ.get("RUSTFLAGS", os.environ.get(target_key, "")))
config = tomllib.loads(Path(".cargo/config.toml").read_text())
native = config["target"]['cfg(all(target_family = "windows", target_env = "msvc"))']["rustflags"]
flags.extend(native)
print("\x1f".join(flags))
