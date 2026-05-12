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


def _start_resource_sampler(results_dir) -> None:
    """Background thread: sample process RSS and CPU every 0.5s, write resource_samples.json on exit."""
    try:
        import atexit
        import json
        import threading
        from pathlib import Path

        import psutil

        proc = psutil.Process()
        proc.cpu_percent(interval=None)  # prime — first call always returns 0.0

        peak_rss = 0
        peak_swap = 0
        cpu_samples = []
        stop_event = threading.Event()

        def _sample():
            nonlocal peak_rss, peak_swap
            while not stop_event.wait(0.5):
                try:
                    rss = proc.memory_info().rss
                    cpu = proc.cpu_percent(interval=None)
                    swap = psutil.swap_memory().used
                    if rss > peak_rss:
                        peak_rss = rss
                    if swap > peak_swap:
                        peak_swap = swap
                    if cpu > 0:
                        cpu_samples.append(cpu)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    break

        thread = threading.Thread(target=_sample, daemon=True, name="resource-sampler")
        thread.start()

        try:
            net_start = psutil.net_io_counters()
        except Exception:
            net_start = None
        try:
            io_start = proc.io_counters()
        except Exception:
            io_start = None

        def _write():
            stop_event.set()
            thread.join(timeout=2)
            out = {
                "peak_rss_bytes": peak_rss,
                "peak_swap_bytes": peak_swap,
                "peak_cpu_pct": round(max(cpu_samples), 1) if cpu_samples else 0.0,
                "avg_cpu_pct": round(sum(cpu_samples) / len(cpu_samples), 1) if cpu_samples else 0.0,
                "sample_count": len(cpu_samples),
            }
            if net_start is not None:
                try:
                    net_end = psutil.net_io_counters()
                    out["net_bytes_recv"] = net_end.bytes_recv - net_start.bytes_recv
                    out["net_bytes_sent"] = net_end.bytes_sent - net_start.bytes_sent
                except Exception:
                    pass
            if io_start is not None:
                try:
                    io_end = proc.io_counters()
                    out["disk_read_bytes"] = io_end.read_bytes - io_start.read_bytes
                    out["disk_write_bytes"] = io_end.write_bytes - io_start.write_bytes
                except Exception:
                    pass
            Path(results_dir, "resource_samples.json").write_text(json.dumps(out, indent=2))

        atexit.register(_write)

    except ImportError:
        pass


def _write_machine_spec(results_dir) -> None:
    """Write machine hardware/OS info to machine_spec.json in the results directory."""
    import json
    import os
    import platform
    import sys
    from pathlib import Path

    spec = {
        "hostname": platform.node(),
        "os": platform.platform(),
        "python": sys.version.split()[0],
        "cpu": platform.processor() or platform.machine(),
        "cpu_count_logical": os.cpu_count(),
    }

    try:
        import psutil

        spec["cpu_count_physical"] = psutil.cpu_count(logical=False)
        mem = psutil.virtual_memory()
        spec["ram_gb"] = round(mem.total / (1024**3), 1)
        spec["ram_available_gb"] = round(mem.available / (1024**3), 1)
        try:
            freq = psutil.cpu_freq(percpu=False)
            if freq and freq.max:
                spec["cpu_freq_max_ghz"] = round(freq.max / 1000, 2)
        except Exception:
            pass
        try:
            disk = psutil.disk_usage(str(results_dir))
            spec["disk_free_gb"] = round(disk.free / (1024**3), 1)
        except Exception:
            pass
    except ImportError:
        try:
            if platform.system() == "Darwin":
                import subprocess

                mem = int(subprocess.check_output(["sysctl", "-n", "hw.memsize"]).decode().strip())
                spec["ram_gb"] = round(mem / (1024**3), 1)
            elif platform.system() == "Linux":
                with open("/proc/meminfo") as fh:
                    for line in fh:
                        if line.startswith("MemTotal:"):
                            spec["ram_gb"] = round(int(line.split()[1]) / (1024**2), 1)
                            break
        except Exception:
            pass
        try:
            import shutil

            disk = shutil.disk_usage(str(results_dir))
            spec["disk_free_gb"] = round(disk.free / (1024**3), 1)
        except Exception:
            pass

    Path(results_dir, "machine_spec.json").write_text(json.dumps(spec, indent=2))


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
            results_url = os.environ.get("ECOSCOPE_WORKFLOWS_RESULTS", "")
            if results_url.startswith("file://"):
                results_dir = Path(urlparse(results_url).path)
                _write_machine_spec(results_dir)
                _start_resource_sampler(results_dir)
                if exporter is None:
                    from ecoscope_workflows_core.tracing import make_otel_console_exporter_file_dst_kws

                    exporter = "console"
                    exporter_kws = make_otel_console_exporter_file_dst_kws(results_dir)
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
