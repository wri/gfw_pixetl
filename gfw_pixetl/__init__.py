import logging
import os
from logging import Logger

if "ENV" in os.environ:
    ENV: str = os.environ["ENV"]
else:
    ENV = "dev"


def get_module_logger(name) -> Logger:
    log = logging.getLogger(name)
    return log
