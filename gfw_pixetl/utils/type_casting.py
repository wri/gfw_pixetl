from typing import Optional

from gfw_pixetl import get_module_logger

LOGGER = get_module_logger(__name__)


def to_bool(value: Optional[str]) -> Optional[bool]:
    boolean = {
        "false": False,
        "true": True,
        "no": False,
        "yes": True,
        "0": False,
        "1": True,
    }
    if value is None:
        response = None
    else:
        try:
            response = boolean[value.lower()]
        except KeyError:
            raise ValueError(f"Cannot convert value {value} to boolean")

    return response


def replace_inf_nan(number: float, replacement: float) -> float:
    if number == float("inf") or number == float("nan"):
        LOGGER.debug("Replace number")
        return replacement
    else:
        return number
