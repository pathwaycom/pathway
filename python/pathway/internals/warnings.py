# Copyright © 2025 Pathway

import warnings

warnings.filterwarnings(
    "ignore",
    message="Deprecated call to `pkg_resources.declare_namespace",
    category=DeprecationWarning,
)
warnings.filterwarnings(
    "ignore",
    message="pkg_resources is deprecated as an API",
    category=DeprecationWarning,
)
