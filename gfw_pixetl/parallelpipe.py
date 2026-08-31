# This file is a modified version of https://github.com/gtsystem/parallelpipe
# intended to add awareness of whether or not a task has been killed
# by the OOM killer.
"""This class provide a transparent way to use multi step map reduce task.

         / map - map2 - reduce
producer - map - map2 /
         \ map /

OOM-killer resilience
---------------------
When the Linux OOM killer sends SIGKILL to a worker process the process
disappears instantly (exit code -9 or 137).  The original code would then
hang forever waiting for EXIT sentinels that the dead worker can never send.

This version adds two things:

1. A background watchdog thread that monitors every worker process.
   When it detects a dead worker it:
     a. Injects the missing EXIT sentinels into the output queue so that
        downstream stages and the main thread never block.
     b. Records an OomKillEvent on the error queue so the caller knows
        which stage was affected.

2. OomKillEvent / OomKillException types that let callers (e.g. PixETL's
   _process_pipe) distinguish an OOM kill from an ordinary exception and
   react accordingly (typically: retry with fewer workers).
"""

import threading
import time
from collections.abc import Iterable
from multiprocessing import Process, Queue

import dill

# ---------------------------------------------------------------------------
# Public exception types
# ---------------------------------------------------------------------------


class OomKillEvent:
    """Placed on the error queue when a worker is OOM-killed."""

    def __init__(self, process_name: str, stage_name: str, missing_exits: int):
        self.process_name = process_name
        self.stage_name = stage_name
        self.missing_exits = missing_exits  # number of EXIT sentinels injected

    def __repr__(self):
        return (
            f"OomKillEvent(process={self.process_name!r}, "
            f"stage={self.stage_name!r}, "
            f"missing_exits={self.missing_exits})"
        )


class OomKillException(Exception):
    """Raised by Pipeline.results() when one or more workers were OOM-
    killed."""

    def __init__(self, events):
        self.events = events  # list[OomKillEvent]
        names = ", ".join(e.stage_name for e in events)
        super().__init__(
            f"{len(events)} worker(s) were OOM-killed in stage(s): {names}. "
            "Retry with fewer workers."
        )


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def identity(x):
    return x


class EXIT:
    """Sentinel: placed in a queue to signal that one sender has finished."""

    pass


def iterqueue(queue, expected):
    """Yield all values from *queue* until *expected* EXIT sentinels arrive."""
    while expected > 0:
        for item in iter(queue.get, EXIT):
            yield item
        expected -= 1


class Task(Process):
    """One worker process that runs a callable in a subprocess."""

    def __init__(self, callable, args=(), kwargs={}):
        super(Task, self).__init__()
        self._callable = dill.dumps(callable)
        self._args = args
        self._kwargs = kwargs
        self._que_in = None
        self._que_out = None
        self._que_err = None

    def set_in(self, que_in, num_senders):
        self._que_in = que_in
        self._num_senders = num_senders

    def set_out(self, que_out, num_followers):
        self._que_out = que_out
        self._num_followers = num_followers

    def set_err(self, que_err):
        self._que_err = que_err

    def _consume(self):
        if self._que_in:
            return iterqueue(self._que_in, self._num_senders)
        return None

    def run(self):
        input = self._consume()
        put_item = self._que_out.put
        func = dill.loads(self._callable)
        try:
            if input is None:  # producer
                res = func(*self._args, **self._kwargs)
            else:
                res = func(input, *self._args, **self._kwargs)
            if res is not None:
                for item in res:
                    put_item(item)

        except Exception as e:
            self._que_err.put((self.name, e))
            if input is not None:
                for _ in input:
                    pass
            raise

        finally:
            for _ in range(self._num_followers):
                put_item(EXIT)
            self._que_err.put(EXIT)
            # Wait until queues are drained before the process exits so the
            # parent's queue objects don't lose buffered data.
            while not self._que_out.empty():
                time.sleep(0.1)
            while not self._que_err.empty():
                time.sleep(0.1)


# ---------------------------------------------------------------------------
# Watchdog
# ---------------------------------------------------------------------------


def _is_oom_killed(process: Process) -> bool:
    """Return True if *process* was killed by SIGKILL / OOM killer."""
    ec = process.exitcode
    # exitcode is None while the process is still alive.
    # -9 means killed by signal 9 (SIGKILL); some kernels/container runtimes
    # report this as 137 (128 + 9) instead.
    return ec is not None and ec not in (0,) and (ec == -9 or ec == 137)


class _StageWatchdog(threading.Thread):
    """Background thread that watches all processes in one stage.

    When an OOM kill is detected the watchdog:
      - injects the missing EXIT sentinels into the output queue
      - puts an OomKillEvent on the error queue
      - puts EXIT on the error queue (so the caller's error-queue count stays
        consistent — one EXIT per worker whether it died normally or was killed)
    """

    def __init__(self, stage: "Stage", out_queue: Queue, err_queue: Queue):
        super().__init__(daemon=True)
        self.stage = stage
        self.out_queue = out_queue
        self.err_queue = err_queue
        # How many EXIT tokens does each worker normally send to out_queue?
        # This is set by Stage.set_out() via num_followers.
        self._num_followers: int = 0
        self._stop_event = threading.Event()

    def set_num_followers(self, num_followers: int):
        self._num_followers = num_followers

    def stop(self):
        self._stop_event.set()

    def run(self):
        watched = list(self.stage.processes)  # snapshot
        handled: set = set()

        while not self._stop_event.is_set():
            for p in watched:
                if p.pid is None:
                    continue  # not started yet
                if id(p) in handled:
                    continue
                if _is_oom_killed(p):
                    handled.add(id(p))
                    # Inject missing sentinels so downstream never blocks
                    for _ in range(self._num_followers):
                        self.out_queue.put(EXIT)
                    event = OomKillEvent(
                        process_name=p.name,
                        stage_name=self.stage.target_name,
                        missing_exits=self._num_followers,
                    )
                    self.err_queue.put(event)
                    # Also put EXIT so error-queue consumer accounts for this worker
                    self.err_queue.put(EXIT)
            time.sleep(0.2)


# ---------------------------------------------------------------------------
# Stage
# ---------------------------------------------------------------------------


class Stage(object):
    """A pool of parallel worker processes all running the same callable."""

    def __init__(self, target, *args, **kwargs):
        if not callable(target):
            raise TypeError("Target is not callable")
        self.qsize = 0
        self.workers = 1
        self._processes = None
        self._target = target
        self._args = args
        self._kwargs = kwargs
        self._num_followers = 1  # updated by Pipeline when wiring queues
        if hasattr(target, "__self__"):
            self.target_name = "%s.%s" % (
                target.__self__.__class__.__name__,
                target.__name__,
            )
        else:
            self.target_name = target.__name__

    def setup(self, workers=1, qsize=0):
        if workers <= 0:
            raise ValueError("workers have to be greater then zero")
        if qsize < 0:
            raise ValueError("qsize have to be greater or equal zero")
        self.qsize = qsize
        self.workers = workers
        return self

    @property
    def processes(self):
        if self._processes is None:
            self._processes = []
            for p in range(self.workers):
                t = Task(self._target, self._args, self._kwargs)
                t.name = "%s-%d" % (self.target_name, p)
                self._processes.append(t)
        return self._processes

    def set_in(self, que_in, num_senders):
        for p in self.processes:
            p.set_in(que_in, num_senders)

    def set_out(self, que_out, num_followers):
        self._num_followers = num_followers
        for p in self.processes:
            p.set_out(que_out, num_followers)

    def set_err(self, que_err):
        for p in self.processes:
            p.set_err(que_err)

    def _start(self):
        for p in self.processes:
            p.start()

    def _join(self):
        for p in self.processes:
            p.join()
        self._processes = None

    def __str__(self):
        return "%s(x%d)" % (self.target_name, self.workers)

    __repr__ = __str__

    def __or__(self, b):
        return Pipeline([self, b])

    def __ror__(self, b):
        if isinstance(b, Iterable):
            return Stage(identity, b) | self
        raise ValueError("Pipe input have to be iterable")

    def results(self):
        return Pipeline([self]).results()

    def execute(self):
        return Pipeline([self]).execute()

    def __call__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        return self


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


class Pipeline(list):
    """An ordered list of connected Stages."""

    def __or__(self, b):
        if isinstance(b, Stage):
            self.append(b)
        elif isinstance(b, Pipeline):
            self.extend(b)
        return self

    def results(self):
        """Wire up all stages, start workers, yield results, raise on error."""
        # ------------------------------------------------------------------ #
        # Wire inter-stage queues
        # ------------------------------------------------------------------ #
        tt = None
        for i, tf in enumerate(self[:-1]):
            tt = self[i + 1]
            q = Queue(tf.qsize)
            tf.set_out(q, tt.workers)
            tt.set_in(q, tf.workers)

        if tt is None:
            tt = self[0]

        # Final output queue (feeds the main thread)
        out_q = Queue(tt.qsize)
        err_q = Queue()
        tt.set_out(out_q, 1)

        # Total number of EXIT tokens we expect on err_q:
        # each worker sends exactly one EXIT (or the watchdog sends it for OOM-
        # killed workers, see _StageWatchdog).
        total_workers = sum(t.workers for t in self)

        # ------------------------------------------------------------------ #
        # Start watchdogs (one per stage, before workers so they're ready)
        # ------------------------------------------------------------------ #
        watchdogs = []
        for stg in self:
            wd = _StageWatchdog(
                stg, stg._processes[0]._que_out if stg.processes else out_q, err_q
            )
            # Reconstruct: the watchdog needs the actual queue and follower count
            # We'll set these properly below after the stages have been wired.
            watchdogs.append((wd, stg))

        # Rebuild watchdogs now that wiring is done (processes have their queues set)
        watchdogs = []
        for stg in self:
            # The output queue for this stage is stored on its processes
            stage_out_q = stg.processes[0]._que_out  # same for all workers in stage
            wd = _StageWatchdog(stg, stage_out_q, err_q)
            wd.set_num_followers(stg.processes[0]._num_followers)
            watchdogs.append(wd)

        # Set the error queue on all stages (must happen before _start)
        for stg in self:
            stg.set_err(err_q)

        # Start watchdogs first so they're ready before any worker can die
        for wd in watchdogs:
            wd.start()

        # Start worker processes
        for stg in self:
            stg._start()

        # ------------------------------------------------------------------ #
        # Yield results from the final output queue
        # ------------------------------------------------------------------ #
        for item in iterqueue(out_q, tt.workers):
            yield item

        # ------------------------------------------------------------------ #
        # Collect errors / OOM events from error queue
        # ------------------------------------------------------------------ #
        raw_errors = list(iterqueue(err_q, total_workers))

        # Stop watchdogs
        for wd in watchdogs:
            wd.stop()

        # Join all worker processes
        for stg in self:
            stg._join()

        # ------------------------------------------------------------------ #
        # Separate OOM events from regular exceptions
        # ------------------------------------------------------------------ #
        oom_events = [e for e in raw_errors if isinstance(e, OomKillEvent)]
        exceptions = [
            (n, e) for n, e in (r for r in raw_errors if isinstance(r, tuple))
        ]

        if oom_events:
            raise OomKillException(oom_events)

        if exceptions:
            task_name, ex = exceptions[0]
            if len(exceptions) == 1:
                msg = 'The task "%s" raised %s' % (task_name, repr(ex))
            else:
                msg = '%d tasks raised an exception. First on task "%s": %s' % (
                    len(exceptions),
                    task_name,
                    repr(ex),
                )
            raise TaskException(msg)

    def execute(self):
        l = None
        for l in self.results():
            pass
        return l


# ---------------------------------------------------------------------------
# Decorators
# ---------------------------------------------------------------------------


def stage(workers=1, qsize=0):
    def decorator(f):
        return Stage(f).setup(workers=workers, qsize=qsize)

    return decorator


def map_stage(workers=1, qsize=0, filter_errors=False):
    def decorator(f):
        if filter_errors:

            def map_task(it, *args, **argv):
                for item in it:
                    try:
                        yield f(item, *args, **argv)
                    except Exception:
                        pass

        else:

            def map_task(it, *args, **argv):
                for item in it:
                    yield f(item, *args, **argv)

        map_task.__name__ = "pipe_map-%s" % f.__name__
        return Stage(map_task).setup(workers=workers, qsize=qsize)

    return decorator


class TaskException(Exception):
    """Exception propagated from one of the tasks."""

    pass
