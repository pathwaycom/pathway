# Copyright © 2024 Pathway

from __future__ import annotations

import json
import subprocess
import sys
from importlib.abc import MetaPathFinder
from importlib.util import spec_from_file_location
from os import environ
from pathlib import Path

PROFILE_ENV_VAR = "PATHWAY_PROFILE"
QUIET_ENV_VAR = "PATHWAY_QUIET"
FEATURES_ENV_VAR = "PATHWAY_FEATURES"
DEFAULT_PROFILE = "dev"

RUST_PACKAGE = "pathway"
MODULE_NAME = "pathway.engine"


class MagicCargoFinder(MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname != MODULE_NAME:
            return None

        module_path = cargo_build()
        return spec_from_file_location(MODULE_NAME, module_path)


def cargo_build():
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
    base_dir = Path(__file__).parent.parent
    assert not (base_dir / "__init__.py").exists()
    cargo = subprocess.run(
        args,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        cwd=base_dir,
        text=True,
        check=True,
    )
    module_file = None
    for line in cargo.stdout.splitlines():
        data = json.loads(line)
        if data["reason"] != "compiler-artifact":
            continue
        if not data["package_id"].startswith(RUST_PACKAGE + " "):
            continue
        for filename in data["filenames"]:
            path = Path(filename)
            if path.suffix != ".so":
                continue
            module_file = path
    assert module_file is not None
    return module_file


sys.meta_path.append(MagicCargoFinder())
