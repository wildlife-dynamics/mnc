"""
Activated automatically by Python at startup when dev/ is on PYTHONPATH.
Configures logging based on ECOSCOPE_LOG_LEVEL env var (default: WARNING).

Set ECOSCOPE_LOG_LEVEL=INFO to see task execution order from graph.py.
Set ECOSCOPE_LOG_LEVEL=DEBUG to see full framework internals.
"""

import logging
import os

level_name = os.environ.get("ECOSCOPE_LOG_LEVEL", "WARNING").upper()
level = getattr(logging, level_name, logging.WARNING)

logging.basicConfig(
    level=level,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)


def _patch_configure_tracer():
    """Patch configure_tracer to auto-enable file-based tracing.

    When ECOSCOPE_WORKFLOWS_RESULTS is a file:// URL and no exporter is explicitly
    configured, automatically write traces to otel_traces.jsonl in that directory.
    This means --otel-exporter and --otel-console-exporter-dst flags are not required.
    """
    try:
        import os
        from pathlib import Path
        from urllib.parse import urlparse

        import ecoscope_workflows_core.tracing._config as _config_mod
        import ecoscope_workflows_core.tracing as _tracing_mod

        _original = _config_mod.configure_tracer

        def _patched(name, version="", exporter=None, exporter_kws=None):
            if exporter is None:
                results_url = os.environ.get("ECOSCOPE_WORKFLOWS_RESULTS", "")
                if results_url.startswith("file://"):
                    from ecoscope_workflows_core.tracing import make_otel_console_exporter_file_dst_kws

                    exporter = "console"
                    exporter_kws = make_otel_console_exporter_file_dst_kws(Path(urlparse(results_url).path))
            _original(name, version=version, exporter=exporter, exporter_kws=exporter_kws or {})

        _config_mod.configure_tracer = _patched
        _tracing_mod.configure_tracer = _patched

    except ImportError:
        pass


def _patch_ecoscope_tracing():
    """Patch ecoscope_workflows_core tracing to set proper OTEL span status codes.

    The upstream library leaves all span statuses as UNSET and does not record
    exceptions. This patch ensures:
    - Successful spans get StatusCode.OK
    - Failed spans get StatusCode.ERROR with the exception recorded as a span event
    - Inner function spans include the task_instance_id as an attribute for easier correlation
    """
    try:
        from dataclasses import replace as _replace
        from functools import wraps

        from opentelemetry import trace
        from opentelemetry.trace import StatusCode

        import ecoscope_workflows_core.tracing._decorator as _dec_mod
        import ecoscope_workflows_core.tracing as _tracing_mod

        def _patched_with_tracing(func, task_instance_id=None):
            @wraps(func)
            def wrapper(*args, **kwargs):
                attributes = {
                    "func.__module__": func.__module__,
                    "func.__name__": func.__name__,
                }
                if task_instance_id:
                    attributes["task_instance_id"] = task_instance_id
                tracer = trace.get_tracer(_dec_mod.__name__)
                with tracer.start_as_current_span(func.__name__, attributes=attributes) as span:
                    try:
                        result = func(*args, **kwargs)
                        span.set_status(StatusCode.OK)
                        return result
                    except Exception as exc:
                        span.record_exception(exc)
                        span.set_status(StatusCode.ERROR, str(exc))
                        raise

            return wrapper

        _dec_mod.with_tracing = _patched_with_tracing
        _tracing_mod.with_tracing = _patched_with_tracing

        from ecoscope_workflows_core.decorators import (
            SyncTask,
            _Task,
            _create_kwargs_iterable,
            _create_mapvalues_kwargs_iterable,
            _get_defaults,
            _wrap_for_mapvalues,
        )

        _tracer_name = "ecoscope_workflows_core.decorators"

        def _patched_task_with_tracing(self):
            return _replace(
                self,
                func=_patched_with_tracing(self.func, task_instance_id=self.task_instance_id),
            )

        _Task.with_tracing = _patched_task_with_tracing

        def _patched_call(self, *args, **kwargs):
            tracer = trace.get_tracer(_tracer_name)
            with tracer.start_as_current_span(
                self.task_instance_id or "",
                attributes={"method": "call"},
            ) as span:
                try:
                    result = self(*args, **kwargs)
                    span.set_status(StatusCode.OK)
                    return result
                except Exception as exc:
                    span.record_exception(exc)
                    span.set_status(StatusCode.ERROR, str(exc))
                    raise

        def _patched_map(self, argnames, argvalues):
            defaults = _get_defaults(self.func)
            kwargs_iterable = _create_kwargs_iterable(argnames, argvalues, defaults)
            tracer = trace.get_tracer(_tracer_name)
            with tracer.start_as_current_span(
                self.task_instance_id or "",
                attributes={"ncalls": str(len(kwargs_iterable)), "method": "map"},
            ) as span:
                try:
                    result = self.executor.map(lambda kw: self.func(**kw), kwargs_iterable)
                    span.set_status(StatusCode.OK)
                    return result
                except Exception as exc:
                    span.record_exception(exc)
                    span.set_status(StatusCode.ERROR, str(exc))
                    raise

        def _patched_mapvalues(self, argnames, argvalues):
            defaults = _get_defaults(self.func)
            kwargs_iterable = _create_mapvalues_kwargs_iterable(argnames, argvalues, defaults)
            tracer = trace.get_tracer(_tracer_name)
            with tracer.start_as_current_span(
                self.task_instance_id or "",
                attributes={"ncalls": str(len(kwargs_iterable)), "method": "mapvalues"},
            ) as span:
                try:
                    result = self.executor.map(_wrap_for_mapvalues(self.func), kwargs_iterable)
                    span.set_status(StatusCode.OK)
                    return result
                except Exception as exc:
                    span.record_exception(exc)
                    span.set_status(StatusCode.ERROR, str(exc))
                    raise

        SyncTask.call = _patched_call
        SyncTask.map = _patched_map
        SyncTask.mapvalues = _patched_mapvalues

    except ImportError:
        pass


_patch_configure_tracer()
_patch_ecoscope_tracing()
