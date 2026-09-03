"""Kairo's project-facing runtime mechanics.

The SDK deliberately knows how to coordinate checkpoint publication, but not
how a model, optimizer, dataloader, or metric is represented.
"""

from .atomic import atomic_publish
from .runtime import AttemptSession, CommandContext, ProgressEnvelope, SuspendResult
from .torch import DistributedAdapter

__all__ = [
    "AttemptSession",
    "CommandContext",
    "DistributedAdapter",
    "ProgressEnvelope",
    "SuspendResult",
    "atomic_publish",
]
