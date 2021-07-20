import os
import signal
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
    with pytest.raises(SubprocessKilledError):
        _ = terminating_proc()
