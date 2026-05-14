#!/usr/bin/env python3
"""Wraps the workflow CLI with a thread-pool-based local executor.

When --execution-mode async is used, replaces LithopsExecutor with a Python
ThreadPoolExecutor so independent task branches run concurrently on local cores.
Sequential mode is unaffected — the patch only applies to LithopsExecutor.

Usage:
  python thread-executor.py <workflow_module> [cli args...]

Example:
  python thread-executor.py ecoscope_workflows_mara_north_event_report_workflow \
      run --config-file params.yaml --execution-mode async --mock-io

Workers default to os.cpu_count(). Override with ECOSCOPE_MAX_WORKERS env var.
"""

import concurrent.futures
import os
import runpy
import sys
from dataclasses import dataclass
from typing import Callable, Iterable, Sequence


_pool: concurrent.futures.ThreadPoolExecutor | None = None


def _patch_executor() -> None:
    """Replace LithopsExecutor with a ThreadPoolExecutor-backed AsyncExecutor."""
    global _pool
    from ecoscope_workflows_core.executors.base import (
        AsyncExecutor,
        Future,
        FutureSequence,
    )
    import ecoscope_workflows_core.executors as _executors_pkg

    try:
        import ecoscope_workflows_core.executors.lithops as _lithops_mod
    except ImportError:
        return  # lithops not installed, nothing to patch

    max_workers = int(os.environ.get("ECOSCOPE_MAX_WORKERS", "") or os.cpu_count() or 4)
    _pool = concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers,
        thread_name_prefix="ecoscope-task",
    )

    @dataclass
    class _LocalFuture(Future):
        _f: concurrent.futures.Future

        def gather(self, *args, **kwargs):
            return self._f.result()

    @dataclass
    class _LocalFutureSequence(FutureSequence):
        _fs: list

        def gather(self, *args, **kwargs) -> Sequence:
            return [f.result() for f in self._fs]

    @dataclass
    class _LocalExecutor(AsyncExecutor):
        def call(self, func: Callable, **kwargs) -> _LocalFuture:
            return _LocalFuture(_f=_pool.submit(func, **kwargs))

        def map(self, func: Callable, iterable: Iterable) -> _LocalFutureSequence:
            return _LocalFutureSequence(_fs=[_pool.submit(func, item) for item in iterable])

    _lithops_mod.LithopsExecutor = _LocalExecutor
    _executors_pkg.LithopsExecutor = _LocalExecutor


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {__file__} <workflow_module> [cli args...]", file=sys.stderr)
        sys.exit(2)

    workflow_module = sys.argv[1]
    # Shift argv so Click sees the remaining args as if the CLI were invoked directly
    argv = [f"{workflow_module}.cli"] + sys.argv[2:]

    # Force async execution mode so the ThreadPoolExecutor patch is active.
    # The desktop app passes --execution-mode sequential by default; we override
    # it here because sequential mode uses PythonExecutor, bypassing our patch.
    # Set ECOSCOPE_FORCE_SEQUENTIAL=1 to disable this override.
    if os.environ.get("ECOSCOPE_FORCE_SEQUENTIAL") != "1":
        try:
            idx = argv.index("--execution-mode")
            if argv[idx + 1] == "sequential":
                argv[idx + 1] = "async"
        except (ValueError, IndexError):
            pass

    sys.argv = argv
    _patch_executor()
    runpy.run_module(f"{workflow_module}.cli", run_name="__main__")
    # Wait for any in-flight thread pool tasks (e.g. generate_report) to finish
    # before the process exits. Worker threads are daemon threads and would be
    # killed immediately on exit without this explicit shutdown.
    if _pool is not None:
        _pool.shutdown(wait=True)

    # Print machine spec and per-task timing summary once all tasks are done.
    _results_env = os.environ.get("ECOSCOPE_WORKFLOWS_RESULTS", "")
    _results_dir = _results_env[len("file://") :] if _results_env.startswith("file://") else _results_env
    _traces = os.path.join(_results_dir, "otel_traces.jsonl") if _results_dir else ""
    if _traces and os.path.isfile(_traces):
        _here = os.path.dirname(os.path.abspath(__file__))
        _parse_script = os.path.join(_here, "parse-traces.py")
        if os.path.isfile(_parse_script):
            import importlib.util as _ilu

            _spec = _ilu.spec_from_file_location("_parse_traces", _parse_script)
            _mod = _ilu.module_from_spec(_spec)
            _spec.loader.exec_module(_mod)
            _mod.main(_traces)
