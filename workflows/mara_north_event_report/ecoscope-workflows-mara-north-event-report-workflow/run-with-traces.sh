#!/bin/bash
python -m ecoscope_workflows_mara_north_event_report_workflow.cli "$@"
ec=$?
rp="${ECOSCOPE_WORKFLOWS_RESULTS#file://}"
traces="$rp/otel_traces.jsonl"
if [ -f "$traces" ]; then
    python "$PIXI_PROJECT_ROOT/parse-traces.py" "$traces"
fi
exit $ec
