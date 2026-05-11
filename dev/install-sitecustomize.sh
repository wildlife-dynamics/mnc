#!/bin/bash
# Re-installs sitecustomize.py into each workflow's default pixi env site-packages.
# Run this after `pixi clean` + `pixi install` wipes the envs.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE="$(dirname "$SCRIPT_DIR")/workflows"
CONTENT=$(cat << 'PYEOF'
"""Auto-loaded by Python at startup. Finds and runs dev/sitecustomize.py from the mnc repo root.

This file lives in the pixi env site-packages so it is loaded for every Python
invocation in this environment, including direct `pixi run` calls that do not
set PYTHONPATH to include dev/. It walks up the directory tree from __file__
to locate dev/sitecustomize.py, then loads it via importlib to avoid conflicting
with the already-in-progress import of this `sitecustomize` module.
"""
import importlib.util
from pathlib import Path

for _parent in Path(__file__).resolve().parents:
    _candidate = _parent / "dev" / "sitecustomize.py"
    if _candidate.exists():
        _spec = importlib.util.spec_from_file_location("_mnc_dev_sitecustomize", _candidate)
        _mod = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_mod)
        break
PYEOF
)

installed=0
skipped=0

for workflow_dir in "$BASE"/*/; do
    for pkg_dir in "$workflow_dir"*/; do
        site_pkgs=$(find "$pkg_dir.pixi/envs/default" -name "site-packages" -path "*/python3.*/*" -not -path "*__clobbers__*" -type d 2>/dev/null | head -1)
        if [ -n "$site_pkgs" ]; then
            dest="$site_pkgs/sitecustomize.py"
            printf '%s\n' "$CONTENT" > "$dest"
            echo "installed: $dest"
            ((installed++))
        else
            ((skipped++))
        fi
    done
done

echo ""
echo "Done: $installed installed, $skipped skipped (no env yet)"
