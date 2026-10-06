"""Module independence tests — each .py must import without the others.

Root cause this guards against: circular imports that only work when
bridge.py loads first (masking the cycle by pre-populating sys.modules).
The old test suite always did `import bridge` before `import telegram`,
so the circular import in v0.47.0 went undetected for months.

These tests spawn a fresh Python subprocess per module — no import-order
tricks, no shared sys.modules state.
"""

import subprocess
import sys
import pytest


# Each module that must be independently importable.
INDEPENDENT_MODULES = ["core", "telegram", "claudecode"]


@pytest.mark.parametrize("module", INDEPENDENT_MODULES)
def test_module_imports_independently(module):
    """Module can be imported in a clean Python process without bridge.py."""
    result = subprocess.run(
        [sys.executable, "-c", f"import {module}"],
        capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == 0, (
        f"{module}.py failed to import independently:\n{result.stderr}"
    )


@pytest.mark.parametrize("module", INDEPENDENT_MODULES)
def test_module_imports_before_bridge(module):
    """Module can be imported BEFORE bridge.py (not just after)."""
    result = subprocess.run(
        [sys.executable, "-c", f"import {module}; import bridge"],
        capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == 0, (
        f"{module}.py failed when imported before bridge:\n{result.stderr}"
    )


def test_no_circular_import_any_order():
    """All three modules import in any order without circular errors."""
    import itertools
    for perm in itertools.permutations(INDEPENDENT_MODULES + ["bridge"]):
        imports = "; ".join(f"import {m}" for m in perm)
        result = subprocess.run(
            [sys.executable, "-c", imports],
            capture_output=True, text=True, timeout=10,
        )
        assert result.returncode == 0, (
            f"Import order {' → '.join(perm)} failed:\n{result.stderr}"
        )
