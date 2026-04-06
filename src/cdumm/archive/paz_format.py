"""PAZ directory name predicates."""


def is_paz_dir(name: str) -> bool:
    """Return True if *name* is a 4-digit numeric PAZ directory name."""
    return name.isdigit() and len(name) == 4


def is_mod_dir(name: str) -> bool:
    """Return True if *name* is a 4-digit numeric mod directory (index >= 36)."""
    return is_paz_dir(name) and int(name) >= 36
