# Copyright © 2023 Pathway

import os

ignore_asserts = os.environ.get("PATHWAY_IGNORE_ASSERTS", "false").lower() in (
    "1",
    "true",
    "yes",
)
