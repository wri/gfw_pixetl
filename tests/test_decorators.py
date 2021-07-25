import os
import signal
from concurrent import futures
from time import sleep

import pytest

from gfw_pixetl.decorators import SubprocessKilledError, processify


class SomeException(Exception):
    pass


@processify
def sleeper_proc(seconds):
    # print("Hi! I'm a sleeper process!")
    sleep(seconds)
    # print("I ran to completion!")
    return 42


@processify
def exception_proc():
    # print("Hi! I'm an exception-raising process!")
    raise SomeException("Boom!")


@processify
def terminating_proc():
    # print("Hi! I'm a self-terminating process!")
    os.kill(os.getpid(), signal.SIGTERM)


def test_normal_exit():
    ret_val = sleeper_proc(3)
    assert ret_val == 42


def test_exception_proc():
    with pytest.raises(SomeException):
        _ = exception_proc()


def test_terminating_proc():
    # Test in a future so if it DOESN'T work we don't have to
    # wait forever (i.e. pytest's timeout) to find out
    with futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(terminating_proc)
        try:
            _ = future.result(timeout=2)
        except SubprocessKilledError:
            pass
        else:
            pytest.fail("Processify never returned")
