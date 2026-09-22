import multiprocessing as mp
import os
import sys
import threading
import traceback
from functools import cached_property, wraps

import dill


class SubprocessKilledError(Exception):
    pass


def lazy_property(fn):
    """Lazy-evaluated property backed by functools.cached_property.

    Behaves identically to the old hand-rolled version but delegates to
    the stdlib implementation, which is well-tested and supports cache
    invalidation via ``del obj.<name>``.  This is important for the OOM
    retry path, where we need to clear cached state that references files
    inside a tile's work directory before that directory is recreated.

    The ``cached_property`` descriptor stores its value in the instance
    ``__dict__`` under the function's own name, so deletion is simply:

        del tile.src               # clears RasterSrcTile.src cache
        del tile.intersecting_window

    Note: ``cached_property`` is not re-entrant; if two threads access the
    same unset property simultaneously they may both compute it.  That is
    fine here because each tile is only ever touched by one worker at a time.
    """
    return cached_property(fn)


def _log_unsafe_fork(kind, target):
    """Log live non-current thread stacks immediately before a fork."""
    threads = threading.enumerate()
    if len(threads) <= 1:
        return

    current_ident = threading.get_ident()
    frames = sys._current_frames()
    details = []

    for thread in threads:
        if thread.ident == current_ident:
            continue

        frame = frames.get(thread.ident)
        stack = (
            "".join(traceback.format_stack(frame)).strip()
            if frame is not None
            else "<stack unavailable>"
        )
        details.append(
            {
                "name": thread.name,
                "ident": thread.ident,
                "daemon": thread.daemon,
                "stack": stack,
            }
        )

    # Import locally to keep the decorator module's existing initialization
    # behavior unchanged.
    from gfw_pixetl import get_module_logger

    get_module_logger(__name__).warning(
        "Unsafe multiprocessing fork: kind=%s target=%s pid=%d "
        "current_thread=%s non_current_threads=%r",
        kind,
        target,
        os.getpid(),
        threading.current_thread().name,
        details,
    )


def _processify_target(q, func_bytes, args, kwargs):
    """Run a processified callable in a spawned child process."""
    func = dill.loads(func_bytes)
    try:
        ret = func(*args, **kwargs)
    except Exception:
        ex_type, ex_value, tb = sys.exc_info()
        error = ex_type, ex_value, "".join(traceback.format_tb(tb))
        ret = None
    else:
        error = None

    q.put((ret, error))


def processify(func):
    """Decorator to run a function synchronously in a spawned process.

    Arguments and return values must be picklable.  ``spawn`` is used
    explicitly so this helper remains safe when native libraries have
    created threads in the parent process.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        ctx = mp.get_context("spawn")
        q = ctx.Queue()
        p = ctx.Process(
            target=_processify_target,
            args=(q, dill.dumps(func), args, kwargs),
        )

        error = None
        ret = None
        untimely_death = False

        p.start()

        while p.is_alive():
            p.join(timeout=60)  # TODO: Make configurable
            exit_code = p.exitcode
            if exit_code is None:
                continue
            if exit_code < 0:
                untimely_death = True
                break

        if not untimely_death and p.exitcode not in (0, None):
            untimely_death = True

        if not untimely_death:
            # Timeout for the improbable case that exception/result delivery
            # itself fails.
            ret, error = q.get(timeout=60)

        q.close()
        q.join_thread()

        if untimely_death:
            raise SubprocessKilledError("Process was killed")
        if error:
            _, ex_value, _ = error
            raise ex_value
        return ret

    return wrapper
