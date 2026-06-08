import json
import os
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import grpc
from . import broker_pb2 as pb
from . import broker_pb2_grpc as pb_grpc

_BAR_INTERVAL = 1.0

# Returned by WorkDaemon.get_state when no value is set for the key. Empty
# state_json ("") is an unambiguous "not set" marker: a stored JSON value is
# never "" (json.dumps("") is '""').
_MISSING = object()


def _broker_addr() -> str:
    ray_address = os.environ.get("RAY_ADDRESS", "")
    host = ray_address.split(":")[0] if ":" in ray_address else "localhost"
    return f"{host}:5001"


class WorkDaemon:
    def __init__(self, work_context: str):
        self._session_id = os.environ.get("MANTA_SESSION_ID", "")
        addr = _broker_addr()
        print(f"[api] connecting to broker at {addr}", flush=True)
        self._channel = grpc.insecure_channel(addr)
        self._stub = pb_grpc.BrokerStub(self._channel)
        self._work_context = work_context
        self._q: queue.SimpleQueue = queue.SimpleQueue()
        self._executor = ThreadPoolExecutor(max_workers=2)
        self._future = self._executor.submit(self._push_stream)
        self._bar_lock = threading.Lock()
        self._bar_last_sent: dict[str, float] = {}
        self._bar_pending: dict[str, pb.ProgressBar] = {}
        print(f"[api] connected, work_context={work_context}", flush=True)

    def _push_stream(self):
        def gen():
            while True:
                item = self._q.get()
                if item is None:
                    return
                yield item
        self._stub.PushEvents(gen())

    def progress_update(self, text: str):
        print(f"[api] progress_update: {text}", flush=True)
        self._q.put(pb.Event(
            work_context=self._work_context,
            session_id=self._session_id,
            type="log",
            text=text,
        ))

    def _send_bar(self, bar: pb.ProgressBar):
        self._executor.submit(self._stub.SetProgressBar, bar)

    def progress_bar(self, name: str, value: float, vmin: float, vmax: float, is_int: bool):
        bar = pb.ProgressBar(
            session_id=self._session_id,
            work_context=self._work_context,
            name=name,
            value=value,
            min=vmin,
            max=vmax,
            is_int=is_int,
        )
        now = time.monotonic()
        send = False
        with self._bar_lock:
            last = self._bar_last_sent.get(name, 0.0)
            if now - last >= _BAR_INTERVAL:
                self._bar_last_sent[name] = now
                self._bar_pending.pop(name, None)
                send = True
            else:
                self._bar_pending[name] = bar
        if send:
            self._send_bar(bar)

    def get_params(self) -> list[dict]:
        resp = self._stub.GetParams(pb.WorkContext(
            value=self._work_context,
            session_id=self._session_id,
        ))
        result = json.loads(resp.params_json) if resp.params_json else []
        return result if result is not None else []

    def set_state(self, state_name: str, value):
        # Synchronous unary RPC: the broker holds the value before this returns,
        # so a downstream work in a later step is guaranteed to see it.
        self._stub.SetState(pb.WorkState(
            work_context=self._work_context,
            session_id=self._session_id,
            state_name=state_name,
            state_json=json.dumps(value),
        ))

    def get_state(self, work_context: str, state_name: str):
        resp = self._stub.GetState(pb.WorkState(
            work_context=work_context,
            session_id=self._session_id,
            state_name=state_name,
        ))
        if resp.state_json == "":
            return _MISSING
        return json.loads(resp.state_json)

    def close(self):
        print("[api] closing", flush=True)
        with self._bar_lock:
            pending = list(self._bar_pending.values())
            self._bar_pending.clear()
        for bar in pending:
            self._send_bar(bar)
        self._q.put(None)
        self._future.result()
        self._executor.shutdown(wait=True)
        self._channel.close()
