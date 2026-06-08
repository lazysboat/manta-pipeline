import os
import functools

from mantapipeline.work_daemon import WorkDaemon, _MISSING

_daemon: WorkDaemon | None = None


def progress_update(text: str):
    if _daemon is not None:
        _daemon.progress_update(text)


def progress_bar(name: str, value, min, max):
    if _daemon is None:
        return
    is_int = isinstance(value, int) and isinstance(min, int) and isinstance(max, int)
    _daemon.progress_bar(name, float(value), float(min), float(max), is_int)


def params() -> dict:
    if _daemon is None:
        return {}
    return {item["name"]: item["value"] for item in _daemon.get_params()}


def set_state(state_name: str, value):
    if _daemon is not None:
        _daemon.set_state(state_name, value)


def get_state(work_context: str, state_name: str, default=None):
    if _daemon is None:
        return default
    val = _daemon.get_state(work_context, state_name)
    return default if val is _MISSING else val


def entrypoint(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        global _daemon
        work_context = os.environ["MANTA_WORK_CONTEXT"]
        _daemon = WorkDaemon(work_context)
        try:
            return fn(*args, **kwargs)
        finally:
            _daemon.close()
            _daemon = None
    return wrapper


def tag(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        # Local import avoids cloudpickle capturing the live gRPC channel
        # when serializing Ray remote functions
        import mantapipeline.api as _api
        work_context = os.environ["MANTA_WORK_CONTEXT"]
        d = WorkDaemon(work_context)
        _api._daemon = d
        try:
            return fn(*args, **kwargs)
        finally:
            d.close()
            _api._daemon = None
    return wrapper
