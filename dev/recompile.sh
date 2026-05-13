#!/bin/bash
set -e

workflow=$1
shift
flags=$*

pixi update --manifest-path pixi.toml -e compile

# (re)initialize dot executable to ensure graphviz is available
pixi run --manifest-path pixi.toml -e compile dot -c

echo "recompiling workflows/${workflow}/spec.yaml with flags '--clobber ${flags}'"

pixi run --manifest-path pixi.toml --locked -e compile \
  ecoscope-workflows compile --spec workflows/${workflow}/spec.yaml --clobber ${flags}

# Patch the generated cli.py to always enable OTEL console tracing writing to
# otel_traces.jsonl in ECOSCOPE_WORKFLOWS_RESULTS (no CLI flags or env vars needed).
workflow_hyphen="${workflow//_/-}"
cli_py="workflows/${workflow}/ecoscope-workflows-${workflow_hyphen}-workflow/ecoscope_workflows_${workflow}_workflow/cli.py"

workflow_dir="workflows/${workflow}/ecoscope-workflows-${workflow_hyphen}-workflow"

if [ -f "$cli_py" ]; then
  echo "Patching ${cli_py} to enable per-task timing by default..."
  sed -i.bak \
    's/    default=None,$/    default="console",/' \
    "$cli_py"
  sed -i.bak \
    's/    default="stdout",$/    default="file",/' \
    "$cli_py"
  rm -f "${cli_py}.bak"
  echo "Patched. Per-task timing will be written to otel_traces.jsonl in ECOSCOPE_WORKFLOWS_RESULTS."
fi

# The async DAG compiler drops empty-list values from partial dicts (compiler bug).
# Patch run_async.py and run_async_mock_io.py to restore event_types: [] for
# get_events_data, which is a required argument with no default in get_events().
dags_dir="workflows/${workflow}/ecoscope-workflows-${workflow_hyphen}-workflow/ecoscope_workflows_${workflow}_workflow/dags"
python3 - "$dags_dir" << 'PYEOF'
import sys, re
from pathlib import Path

dags = Path(sys.argv[1])
MARKER = '"include_display_values": False,'
INSERT = '                "event_types": [],'
files = [dags / "run_async.py", dags / "run_async_mock_io.py"]

for f in files:
    if not f.exists():
        continue
    text = f.read_text()
    modified = False

    # Restore event_types: [] for get_events_data
    if '"event_types"' not in text and MARKER in text:
        text = text.replace(MARKER, f'{INSERT}\n                {MARKER}')
        modified = True
        print(f"{f.name}: added event_types=[] to get_events_data partial")
    else:
        print(f"{f.name}: event_types already present or marker not found, skipping")

    # Restore widgets: [] for mnc_events_dashboard (gather_dashboard requires it)
    WIDGETS_MARKER = '"details": DependsOn("workflow_details"),'
    WIDGETS_INSERT = '                "widgets": [],'
    if '"widgets"' not in text.split('"mnc_events_dashboard"')[1][:300] if '"mnc_events_dashboard"' in text else True:
        if WIDGETS_MARKER in text:
            text = text.replace(WIDGETS_MARKER, f'{WIDGETS_INSERT}\n                {WIDGETS_MARKER}', 1)
            modified = True
            print(f"{f.name}: added widgets=[] to mnc_events_dashboard partial")
    else:
        print(f"{f.name}: widgets already present in mnc_events_dashboard, skipping")

    if modified:
        f.write_text(text)
PYEOF

# The async DAG compiler emits plain list literals for multi-dependency args (e.g.
# static_layers), but gather_dependencies only resolves DependsOnSequence, not list.
# Wrap every list-of-only-DependsOn values with DependsOnSequence(...) and add the
# import so the resolved values reach the task functions correctly.
python3 - "$dags_dir" << 'PYEOF'
import sys, re
from pathlib import Path

dags = Path(sys.argv[1])
files = [dags / "run_async.py", dags / "run_async_mock_io.py"]

IMPORT_OLD = 'from ecoscope_workflows_core.graph import DependsOn, Graph, Node'
IMPORT_NEW = 'from ecoscope_workflows_core.graph import DependsOn, DependsOnSequence, Graph, Node'

# Matches a list literal whose elements are exclusively DependsOn(...) calls
DEPENDS_ON_LIST = re.compile(r'\[(?:\n\s+DependsOn\("[^"]+"\),)+\n\s+\]')

for f in files:
    if not f.exists():
        continue
    text = f.read_text()
    modified = False

    if IMPORT_OLD in text:
        text = text.replace(IMPORT_OLD, IMPORT_NEW)
        modified = True
        print(f"{f.name}: added DependsOnSequence to import")

    new_text = DEPENDS_ON_LIST.sub(lambda m: f'DependsOnSequence({m.group(0)})', text)
    if new_text != text:
        text = new_text
        modified = True
        print(f"{f.name}: wrapped DependsOn list literals with DependsOnSequence")

    if modified:
        f.write_text(text)
    else:
        print(f"{f.name}: no DependsOnSequence changes needed")
PYEOF

# dispatch.py expects the terminal node to return a Pydantic model with model_dump().
# generate_report returns a str, so it must NOT be the terminal node. Add generate_report
# as a topological dependency of mnc_events_dashboard so that mnc_events_dashboard is
# always the last node in the execution graph and returns the Dashboard model.
python3 - "$dags_dir" << 'PYEOF'
import sys
from pathlib import Path

dags = Path(sys.argv[1])
files = [dags / "run_async.py", dags / "run_async_mock_io.py"]

OLD = '"mnc_events_dashboard": ["workflow_details", "time_range", "groupers"],'
NEW = '"mnc_events_dashboard": ["workflow_details", "time_range", "groupers", "generate_report"],'

for f in files:
    if not f.exists():
        continue
    text = f.read_text()
    if OLD in text:
        text = text.replace(OLD, NEW)
        f.write_text(text)
        print(f"{f.name}: mnc_events_dashboard now depends on generate_report")
    elif NEW in text:
        print(f"{f.name}: generate_report dep already present, skipping")
    else:
        print(f"Warning: {f.name}: expected dependency line not found, skipping")
PYEOF

# Copy dev scripts into the workflow package directory so they travel with
# the workflow when the desktop app deploys it to its own template location.
cp "$(dirname "$0")/parse-traces.py" "${workflow_dir}/parse-traces.py"
echo "Copied parse-traces.py into ${workflow_dir}/"
cp "$(dirname "$0")/resource-sampler.py" "${workflow_dir}/resource-sampler.py"
echo "Copied resource-sampler.py into ${workflow_dir}/"
cp "$(dirname "$0")/thread-executor.py" "${workflow_dir}/thread-executor.py"
echo "Copied thread-executor.py into ${workflow_dir}/"

# Generate run-with-traces.sh referencing co-located scripts via
# PIXI_PROJECT_ROOT (set by pixi to the workflow package directory at runtime).
# Pass --execution-mode async to enable local thread-pool parallelism.
wrapper="${workflow_dir}/run-with-traces.sh"
cat > "$wrapper" << WRAPPER_EOF
#!/bin/bash
rp="\${ECOSCOPE_WORKFLOWS_RESULTS#file://}"
if [ -n "\$rp" ]; then
    python "\$PIXI_PROJECT_ROOT/resource-sampler.py" "\$rp" \
        python "\$PIXI_PROJECT_ROOT/thread-executor.py" "ecoscope_workflows_${workflow}_workflow" "\$@"
else
    python "\$PIXI_PROJECT_ROOT/thread-executor.py" "ecoscope_workflows_${workflow}_workflow" "\$@"
fi
exit \$?
WRAPPER_EOF
chmod +x "$wrapper"
echo "Generated ${wrapper}"

# Patch the pixi.toml task to call run-with-traces.sh instead of the CLI directly.
pixi_toml="${workflow_dir}/pixi.toml"
if [ -f "$pixi_toml" ]; then
  python3 - "$pixi_toml" "$workflow" "$workflow_hyphen" << 'PYEOF'
import sys
path, workflow, workflow_hyphen = sys.argv[1], sys.argv[2], sys.argv[3]
old = f'ecoscope-workflows-{workflow_hyphen}-workflow = "python -m ecoscope_workflows_{workflow}_workflow.cli"'
new = f'ecoscope-workflows-{workflow_hyphen}-workflow = "bash run-with-traces.sh"'
content = open(path).read()
if old in content:
    open(path, "w").write(content.replace(old, new))
    print(f"Patched {path}: task now calls run-with-traces.sh")
else:
    print(f"Warning: expected task line not found in {path}, skipping pixi.toml patch")
PYEOF
fi
