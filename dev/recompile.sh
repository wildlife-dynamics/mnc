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

# Copy parse-traces.py into the workflow package directory so it travels with
# the workflow when the desktop app deploys it to its own template location.
cp "$(dirname "$0")/parse-traces.py" "${workflow_dir}/parse-traces.py"
echo "Copied parse-traces.py into ${workflow_dir}/"

# Generate run-with-traces.sh referencing the co-located parse-traces.py via
# PIXI_PROJECT_ROOT (set by pixi to the workflow package directory at runtime).
wrapper="${workflow_dir}/run-with-traces.sh"
cat > "$wrapper" << WRAPPER_EOF
#!/bin/bash
python -m ecoscope_workflows_${workflow}_workflow.cli "\$@"
ec=\$?
rp="\${ECOSCOPE_WORKFLOWS_RESULTS#file://}"
traces="\$rp/otel_traces.jsonl"
if [ -f "\$traces" ]; then
    python "\$PIXI_PROJECT_ROOT/parse-traces.py" "\$traces"
fi
exit \$ec
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
