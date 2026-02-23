import sys
import traceback
from functools import cached_property, wraps
from multiprocessing import Process, Queue


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


def processify(func):
    """Decorator to run a function as a process.

    Be sure that every argument and the return value
    is *picklable*.
    The created process is joined, so the code is
    synchronous.
    Modified from original to not hang when subprocess
    gets killed (such as from OOM).
    Credits: https://gist.github.com/schlamar/2311116
    """

    def process_func(q, *args, **kwargs):
        try:
            ret = func(*args, **kwargs)
        except Exception:
            ex_type, ex_value, tb = sys.exc_info()
            error = ex_type, ex_value, "".join(traceback.format_tb(tb))
            ret = None
        else:
            error = None

        q.put((ret, error))

    # register the original function with a different name
    # in sys.modules so it is picklable
    process_func.__name__ = func.__name__ + "_processify_func"
    setattr(sys.modules[__name__], process_func.__name__, process_func)

    @wraps(func)
    def wrapper(*args, **kwargs):
        q = Queue()
        p = Process(target=process_func, args=(q, *args), kwargs=kwargs)

        error = None
        ret = None
        untimely_death = False

        p.start()

        while p.is_alive():
            p.join(timeout=60)  # TODO: Make configurable
            exit_code = p.exitcode
            if exit_code is None:
                continue
            elif exit_code < 0:
                untimely_death = True
                break
            # Timeout for improbable case the processify exception handling itself fails
            ret, error = q.get(timeout=60)

        if untimely_death:
            raise SubprocessKilledError("Process was killed")
        elif error:
            _, ex_value, _ = error
            raise ex_value
        return ret

    return wrapper
