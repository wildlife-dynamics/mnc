#!/bin/bash
python -m ecoscope_workflows_livestock_monitoring_workflow.cli "$@"
ec=$?
rp="${ECOSCOPE_WORKFLOWS_RESULTS#file://}"
traces="$rp/otel_traces.jsonl"
if [ -f "$traces" ]; then
    python "$PIXI_PROJECT_ROOT/../../../dev/parse-traces.py" "$traces"
fi
exit $ec
