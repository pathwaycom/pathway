#!/bin/bash
set -euo pipefail

# Build the disk-cache example from an empty cache on every documentation refresh.
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
rm -rf -- Cache
