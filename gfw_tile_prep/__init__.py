import logging


def get_module_logger(name):
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.addHandler(sh)
    logger.setLevel(logging.INFO)

    return logger
