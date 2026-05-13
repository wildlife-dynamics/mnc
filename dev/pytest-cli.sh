#!/bin/bash

set -e          # Exit on error
set -o pipefail # Pipe exit code = first failing command, not last (tee)

# Parse arguments
workflow_name=$1
test_case=$2
skip_setup=false

for arg in "$@"; do
    if [ "$arg" = "--skip-setup" ]; then
        skip_setup=true
    fi
done

if [ -z "$workflow_name" ] || [ -z "$test_case" ]; then
    echo "Usage: $0 <workflow_name> <test_case> [--skip-setup]"
    echo "Example: $0 mara_north_event_report all-grouper"
    echo "Options:"
    echo "  --skip-setup    Skip pixi update and playwright-install steps"
    exit 1
fi

workflow_dash=$(echo $workflow_name | tr '_' '-')

# Get absolute paths
repo_root=$(pwd)
workflow_dir="${repo_root}/workflows/${workflow_name}/ecoscope-workflows-${workflow_dash}-workflow"
manifest_path="${workflow_dir}/pixi.toml"
test_cases_file="${repo_root}/workflows/${workflow_name}/test-cases.yaml"

echo "=========================================="
echo "Workflow: $workflow_name"
echo "Test case: $test_case"
echo "=========================================="

# Optional setup steps
if [ "$skip_setup" = false ]; then
    echo "Updating pixi environment..."
    pixi update --manifest-path $manifest_path
    echo "Installing playwright..."
    # Windows-specific: Install playwright package via pip first
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" || "$RUNNER_OS" == "Windows" ]]; then
        echo "Windows detected - installing playwright via pip first..."
        pixi run --manifest-path $manifest_path --locked -e default pip install playwright
    fi
    pixi run --manifest-path $manifest_path --locked -e default bash -c "playwright install --with-deps chromium"
else
    echo "Skipping pixi update and playwright-install (--skip-setup flag provided)"
fi

# Verify test case exists
if ! yq -e ".\"${test_case}\"" "$test_cases_file" > /dev/null 2>&1; then
    echo "ERROR: Test case '${test_case}' not found in $test_cases_file"
    exit 1
fi

# Results directory: use ECOSCOPE_RESULTS_DIR env var if set, otherwise /tmp.
# Set ECOSCOPE_RESULTS_DIR to keep outputs across runs (e.g. export ECOSCOPE_RESULTS_DIR=./output).
# On GitHub Actions, RUNNER_TEMP is used automatically.
if [ -n "$ECOSCOPE_RESULTS_DIR" ]; then
    results_dir="${ECOSCOPE_RESULTS_DIR}/${workflow_name}/${test_case}"
elif [ -n "$RUNNER_TEMP" ]; then
    results_dir="${RUNNER_TEMP}/workflow-test-results/${workflow_name}/${test_case}"
else
    results_dir="/tmp/workflow-test-results/${workflow_name}/${test_case}"
fi
rm -rf "$results_dir"
mkdir -p "$results_dir"
echo "Created results directory: $results_dir"
echo ""

# Export ECOSCOPE_WORKFLOWS_RESULTS for workflow to use
export ECOSCOPE_WORKFLOWS_RESULTS="file://${results_dir}"

# Extract params for this test case
params_file="${results_dir}/params.yaml"
yq ".\"${test_case}\".params" "$test_cases_file" > "$params_file"

echo "Extracted params:"
cat "$params_file"
echo ""

# Run workflow CLI directly
log_file="${results_dir}/workflow.log"
echo "Executing workflow..."
echo "Results will be written to: $ECOSCOPE_WORKFLOWS_RESULTS"
echo "Execution log: $log_file"
echo ""

cd "$workflow_dir"
workflow_underscore=$(echo $workflow_name | tr '-' '_')

# resource-sampler.py wraps the CLI to track peak memory, CPU, disk and network.
# ECOSCOPE_LOG_LEVEL controls verbosity: INFO shows task execution order, DEBUG shows internals.
# stdout+stderr are piped through tee so they appear in terminal AND are saved to workflow.log.
# OTEL flags are always on: traces are written to otel_traces.jsonl in the results dir.
export ECOSCOPE_LOG_LEVEL="${ECOSCOPE_LOG_LEVEL:-INFO}"
pixi run --manifest-path $manifest_path -e default \
    python "${repo_root}/dev/resource-sampler.py" "$results_dir" \
    python -u -m ecoscope_workflows_${workflow_underscore}_workflow.cli run \
    --config-file "$params_file" --execution-mode sequential \
    --mock-io \
    --otel-exporter console --otel-console-exporter-dst file \
    2>&1 | tee "$log_file"

# Validate result.json
result_json="${results_dir}/result.json"
if [ ! -f "$result_json" ]; then
    echo "ERROR: result.json not found at $result_json"
    exit 1
fi

echo ""
echo "Validating result.json..."
error_value=$(jq -r '.error // "null"' "$result_json")

if [ "$error_value" != "null" ]; then
    echo "ERROR: Workflow failed"
    echo "Error details:"
    jq -r '.error' "$result_json"
    echo ""
    echo "Full result.json:"
    cat "$result_json"
    exit 1
fi

echo "Test passed - workflow completed without errors"

# Print per-task timings from OTEL traces
traces_file="${results_dir}/otel_traces.jsonl"
if [ -f "$traces_file" ]; then
    echo ""
    echo "=========================================="
    echo "Task timing:"
    echo "=========================================="
    python3 "${repo_root}/dev/parse-traces.py" "$traces_file"
else
    echo "Warning: otel_traces.jsonl was not found at $traces_file"
fi

echo ""
echo "Full result.json:"
cat "$result_json"