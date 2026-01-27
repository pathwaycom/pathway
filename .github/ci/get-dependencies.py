# Used to run mypy in public repository CI quicker.
#
# Normally, `mypy` requires dependencies to be installed,
# however if one launch the dependencies installation
# via `pip install .`, it would lead to binary compilation
# which is slow.
#
# This script collects the dependencies to do the
# `pip install -r requirements.txt` without the main package
# build.

import tomllib

if __name__ == "__main__":
    with open("pyproject.toml", "rb") as f:
        pyproject = tomllib.load(f)
    deps = pyproject.get("project", {}).get("dependencies", [])
    optional_deps_dict = pyproject.get("project", {}).get("optional-dependencies", {})
    for group_deps in optional_deps_dict.values():
        deps.extend(group_deps)
    deps = [d for d in deps if not d.startswith("pathway")]
    with open("requirements.txt", "w") as f:
        f.write("\n".join(deps))
