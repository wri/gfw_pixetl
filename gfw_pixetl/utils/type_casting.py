from typing import Optional


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
