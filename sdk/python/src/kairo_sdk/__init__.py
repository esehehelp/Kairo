"""Kairo's project-facing runtime mechanics.

The SDK deliberately knows checkpoint and project-selected controller
mechanics, but not how a model, optimizer, dataloader, metric, or workflow
state is represented.
"""

from .atomic import atomic_publish
from .controller import (
    ControllerAPI,
    ControllerDecision,
    ControllerJournal,
    ControllerPolicy,
    KairoControllerClient,
    PolicyDecisionFact,
    ProjectController,
    ResumeAfterKairoPreemption,
    TransientControllerError,
)
from .runtime import AttemptSession, CommandContext, ProgressEnvelope, SuspendResult
from .torch import DistributedAdapter

__all__ = [
    "AttemptSession",
    "CommandContext",
    "ControllerAPI",
    "ControllerDecision",
    "ControllerJournal",
    "ControllerPolicy",
    "DistributedAdapter",
    "KairoControllerClient",
    "PolicyDecisionFact",
    "ProgressEnvelope",
    "ProjectController",
    "ResumeAfterKairoPreemption",
    "SuspendResult",
    "TransientControllerError",
    "atomic_publish",
]
