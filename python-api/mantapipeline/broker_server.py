from concurrent import futures
import queue
import threading
from dataclasses import dataclass, field

import grpc
from . import broker_pb2
from . import broker_pb2_grpc
from google.protobuf import empty_pb2

PORT = 5001


@dataclass
class _BarSub:
    cond: threading.Condition
    dirty: set = field(default_factory=set)  # set of (work_context, name)


class BrokerServicer(broker_pb2_grpc.BrokerServicer):
    def __init__(self):
        self._lock = threading.RLock()
        self._session_queues = {}  # session_id -> Queue
        self._params = {}          # (session_id, work_context) -> params_json
        self._state = {}           # (session_id, work_context, state_name) -> state_json
        self._bars = {}            # (session_id, work_context, name) -> ProgressBar
        self._bar_subscribers = {} # session_id -> list[_BarSub]

    def _session_queue(self, session_id):
        with self._lock:
            if session_id not in self._session_queues:
                self._session_queues[session_id] = queue.Queue()
            return self._session_queues[session_id]

    def PushEvents(self, request_iterator, context):
        for event in request_iterator:
            self._session_queue(event.session_id).put(event)
        return empty_pb2.Empty()

    def Subscribe(self, request, context):
        q = self._session_queue(request.session_id)
        while context.is_active():
            try:
                yield q.get(timeout=1.0)
            except queue.Empty:
                pass

    def RegisterParams(self, request, context):
        with self._lock:
            self._params[(request.session_id, request.work_context)] = request.params_json
        return empty_pb2.Empty()

    def GetParams(self, request, context):
        with self._lock:
            params_json = self._params.get((request.session_id, request.value), "[]")
        return broker_pb2.WorkParams(
            work_context=request.value,
            session_id=request.session_id,
            params_json=params_json,
        )

    def SetState(self, request, context):
        with self._lock:
            self._state[(request.session_id, request.work_context, request.state_name)] = request.state_json
        return empty_pb2.Empty()

    def GetState(self, request, context):
        with self._lock:
            state_json = self._state.get(
                (request.session_id, request.work_context, request.state_name), ""
            )
        return broker_pb2.WorkState(
            work_context=request.work_context,
            state_name=request.state_name,
            session_id=request.session_id,
            state_json=state_json,
        )

    def SetProgressBar(self, request, context):
        key = (request.session_id, request.work_context, request.name)
        with self._lock:
            self._bars[key] = request
            for sub in self._bar_subscribers.get(request.session_id, ()):
                sub.dirty.add((request.work_context, request.name))
                sub.cond.notify()
        return empty_pb2.Empty()

    def SubscribeProgressBars(self, request, context):
        sub = _BarSub(cond=threading.Condition(self._lock))
        with self._lock:
            self._bar_subscribers.setdefault(request.session_id, []).append(sub)
            initial = [
                bar for (sid, _, _), bar in self._bars.items()
                if sid == request.session_id
            ]
        try:
            for bar in initial:
                if not context.is_active():
                    return
                yield bar
            while context.is_active():
                with self._lock:
                    sub.cond.wait_for(lambda: bool(sub.dirty), timeout=1.0)
                    keys = list(sub.dirty)
                    sub.dirty.clear()
                    snapshot = []
                    for wc, name in keys:
                        bar = self._bars.get((request.session_id, wc, name))
                        if bar is not None:
                            snapshot.append(bar)
                for bar in snapshot:
                    if not context.is_active():
                        return
                    yield bar
        finally:
            with self._lock:
                subs = self._bar_subscribers.get(request.session_id, [])
                if sub in subs:
                    subs.remove(sub)


def main():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    broker_pb2_grpc.add_BrokerServicer_to_server(BrokerServicer(), server)
    server.add_insecure_port(f"[::]:{PORT}")
    server.start()
    print("ready", flush=True)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
