#!/bin/bash
rp="${ECOSCOPE_WORKFLOWS_RESULTS#file://}"
if [ -n "$rp" ]; then
    python "$PIXI_PROJECT_ROOT/resource-sampler.py" "$rp"         python "$PIXI_PROJECT_ROOT/thread-executor.py" "ecoscope_workflows_mara_north_event_report_workflow" "$@"
else
    python "$PIXI_PROJECT_ROOT/thread-executor.py" "ecoscope_workflows_mara_north_event_report_workflow" "$@"
fi
exit $?
