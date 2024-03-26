# Copyright © 2024 Pathway

# mypy: disallow-untyped-defs, extra-checks, disallow-any-generics, warn-return-any

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Sequence
from importlib.abc import MetaPathFinder
from importlib.machinery import EXTENSION_SUFFIXES, ModuleSpec
from importlib.util import spec_from_file_location
from os import environ
from pathlib import Path
from types import ModuleType

PROFILE_ENV_VAR = "PATHWAY_PROFILE"
QUIET_ENV_VAR = "PATHWAY_QUIET"
FEATURES_ENV_VAR = "PATHWAY_FEATURES"
DEFAULT_PROFILE = "dev"

RUST_CRATE = "pathway_engine"
MODULE_NAME = "pathway.engine"


class MagicCargoFinder(MetaPathFinder):
    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None,
        target: ModuleType | None = None,
    ) -> ModuleSpec | None:
        if fullname != MODULE_NAME:
            return None

        manifest_dir = Path(__file__).parent.parent.parent
        module_path = cargo_build(manifest_dir)
        return spec_from_file_location(MODULE_NAME, module_path)


def cargo_build(manifest_dir: Path) -> Path:
    assert not (manifest_dir / "__init__.py").exists()
    manifest_file = manifest_dir / "Cargo.toml"
    assert manifest_file.exists()
    profile = environ.get(PROFILE_ENV_VAR, DEFAULT_PROFILE)
    quiet = environ.get(QUIET_ENV_VAR, "0").lower() in ("1", "true", "yes")
    features = environ.get(FEATURES_ENV_VAR)
    args = [
        "cargo",
        "--locked",
        "build",
        "--lib",
        "--message-format=json-render-diagnostics",
        f"--profile={profile}",
    ]
    if quiet:
        args += ["--quiet"]
    if features:
        args += ["--features", features]
    cargo = subprocess.run(
        args,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        cwd=manifest_dir,
        text=True,
        check=True,
    )
    module_candidates = []
    for line in cargo.stdout.splitlines():
        data = json.loads(line)
        if data["reason"] != "compiler-artifact":
            continue
        if data["target"]["name"] != RUST_CRATE:
            continue
        for filename in data["filenames"]:
            path = Path(filename)
            if path.suffix not in EXTENSION_SUFFIXES:
                continue
            module_candidates.append(path)
    assert len(module_candidates) == 1
    return module_candidates[0]


sys.meta_path.append(MagicCargoFinder())
