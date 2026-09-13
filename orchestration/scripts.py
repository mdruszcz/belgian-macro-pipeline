"""Import a module from scripts/ the way the test suite does.

scripts/ is not a package; its modules import each other by bare name
(validate_data imports load_observations_csv), so the directory itself has to
be on sys.path.
"""

import importlib
import sys

from orchestration.paths import REPO_ROOT


def import_script(name: str):
    for path in (REPO_ROOT, REPO_ROOT / "scripts"):
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))
    return importlib.import_module(name)
