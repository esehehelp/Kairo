"""Kairo's project-facing runtime mechanics.

The SDK deliberately knows checkpoint and project-selected controller
mechanics, but not how a model, optimizer, dataloader, metric, or workflow
state is represented.
"""

from . import provenance
from .atomic import atomic_publish
from .client import KairoClient, OperatorSettings, resolve_operator_settings
from .controller import (
    ControllerAPI,
    ControllerConfigurationError,
    ControllerDecision,
    ControllerJournal,
    ControllerPolicy,
    CoordinationEvent,
    KairoControllerClient,
    PolicyDecisionFact,
    ProjectController,
    ResumeAfterKairoPreemption,
    TransientControllerError,
)
from .errors import (
    AttemptFenced,
    AttemptRetired,
    KairoAPIError,
    NoContinuationError,
    classify,
)
from .journal import CommandJournal
from .runtime import (
    AttemptSession,
    CommandContext,
    GangContext,
    ProgressEnvelope,
    ProgressReporter,
    SuspendResult,
    continuation_or,
    install_stop_signals,
    stop_requested,
)
from .torch import DistributedAdapter

__all__ = [
    "AttemptFenced",
    "AttemptRetired",
    "AttemptSession",
    "CommandContext",
    "CommandJournal",
    "ControllerAPI",
    "ControllerConfigurationError",
    "ControllerDecision",
    "ControllerJournal",
    "ControllerPolicy",
    "CoordinationEvent",
    "DistributedAdapter",
    "GangContext",
    "KairoAPIError",
    "KairoClient",
    "KairoControllerClient",
    "NoContinuationError",
    "OperatorSettings",
    "PolicyDecisionFact",
    "ProgressEnvelope",
    "ProgressReporter",
    "ProjectController",
    "ResumeAfterKairoPreemption",
    "SuspendResult",
    "TransientControllerError",
    "atomic_publish",
    "classify",
    "continuation_or",
    "install_stop_signals",
    "provenance",
    "resolve_operator_settings",
    "stop_requested",
]
