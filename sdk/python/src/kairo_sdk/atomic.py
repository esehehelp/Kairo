from __future__ import annotations

import os
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar


T = TypeVar("T")


def atomic_publish(destination: str | Path, write: Callable[[Path], T]) -> T:
    """Publish one file atomically without prescribing its contents.

    ``write`` may be called more than once for the same logical operation. It
    must therefore be safe to rebuild the temporary file. The final replace is
    atomic on the destination filesystem.
    """

    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(
        f".{destination.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
    )
    try:
        result = write(temporary)
        with temporary.open("r+b") as handle:
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        _fsync_directory(destination.parent)
        return result
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _fsync_directory(directory: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
