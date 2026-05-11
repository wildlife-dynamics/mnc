#!/usr/bin/env python3
"""Parse otel_traces.jsonl and print a clean per-task timing summary."""

import json
import sys
from datetime import datetime


def parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def fmt_duration(seconds: float) -> str:
    if seconds < 0.001:
        return f"{seconds * 1000:.1f}ms"
    elif seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    else:
        return f"{seconds:.2f}s"


def main(traces_path: str) -> int:
    spans = []
    with open(traces_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            spans.append(json.loads(line))

    # Index spans by span_id for child lookups
    by_id = {s["context"]["span_id"]: s for s in spans}

    # Root span is the one with no parent (the CLI wrapper span)
    root = next((s for s in spans if s.get("parent_id") is None), None)
    if root is None:
        print("Could not find root span.", file=sys.stderr)
        return 1

    root_id = root["context"]["span_id"]

    # Top-level task spans are direct children of root with method="call"
    tasks = [s for s in spans if s.get("parent_id") == root_id and s.get("attributes", {}).get("method") == "call"]
    tasks.sort(key=lambda s: s["start_time"])

    if not tasks:
        print("No task spans found.", file=sys.stderr)
        return 1

    # For each task span, find its inner function span (child with func.__name__)
    func_name_by_task_id: dict[str, str] = {}
    for s in spans:
        pid = s.get("parent_id")
        fn = s.get("attributes", {}).get("func.__name__")
        if pid and fn and pid in by_id:
            parent = by_id[pid]
            if parent.get("attributes", {}).get("method") == "call":
                func_name_by_task_id[pid] = fn

    workflow_start = parse_ts(tasks[0]["start_time"])

    name_width = max(len(t["name"]) for t in tasks)
    func_width = max(
        (len(func_name_by_task_id.get(t["context"]["span_id"], "")) for t in tasks),
        default=0,
    )
    line_width = max(name_width + func_width + 45, 80)

    total = 0.0
    failed = 0

    print()
    for t in tasks:
        start = parse_ts(t["start_time"])
        end = parse_ts(t["end_time"])
        duration = (end - start).total_seconds()
        offset = (start - workflow_start).total_seconds()
        total += duration

        status_code = t.get("status", {}).get("status_code", "UNSET")
        passed = status_code != "ERROR"
        if not passed:
            failed += 1

        label = "PASSED" if passed else "FAILED"
        fn = func_name_by_task_id.get(t["context"]["span_id"], "")
        fn_str = f"({fn})" if fn else ""
        task_str = f"{t['name']:<{name_width}}  {fn_str:<{func_width + 2}}"
        timing = f"+{offset:6.2f}s  [{fmt_duration(duration):>8}]"
        fixed = len(task_str) + 2 + len(label) + 2 + len(timing)
        dots = "." * max(3, line_width - fixed)
        print(f"{task_str} {dots} {label}  {timing}")

        if not passed:
            exc_event = next(
                (e for e in t.get("events", []) if e.get("name") == "exception"),
                None,
            )
            if exc_event:
                attrs = exc_event.get("attributes", {})
                exc_type = attrs.get("exception.type", "")
                exc_msg = attrs.get("exception.message", "")
                detail = f"{exc_type}: {exc_msg}" if exc_type else exc_msg
            else:
                detail = t.get("status", {}).get("description", "")
            if detail:
                for ln in detail.splitlines()[:5]:
                    print(f"    {ln}")

    passed_count = len(tasks) - failed
    summary = f"{passed_count} passed"
    if failed:
        summary += f", {failed} failed"
    summary += f"  |  total task time {fmt_duration(total)}"
    wall = (parse_ts(tasks[-1]["end_time"]) - workflow_start).total_seconds()
    summary += f"  |  wall clock {fmt_duration(wall)}"

    pad = max(2, (line_width - len(summary) - 2) // 2)
    bar = "=" * pad
    print()
    print(f"{bar} {summary} {bar}")
    print()

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: parse-traces.py <otel_traces.jsonl>", file=sys.stderr)
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
