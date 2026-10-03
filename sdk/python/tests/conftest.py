from __future__ import annotations

import os
from pathlib import Path

import pytest

from kairo_sdk import runtime


@pytest.fixture(autouse=True)
def _isolated_kairo_environment(tmp_path_factory, monkeypatch: pytest.MonkeyPatch):
    """No test sees the developer's Kairo variables, token or CA, or a stop signal.

    Every KAIRO_* variable is removed and the user config dir (where the CLI
    keeps <config>/kairo/token and ca.pem) points at an empty directory.
    """
    for name in list(os.environ):
        if name.startswith("KAIRO_"):
            monkeypatch.delenv(name)
    empty: Path = tmp_path_factory.mktemp("user-config")
    if os.name == "nt":
        monkeypatch.setenv("APPDATA", str(empty))
    else:
        monkeypatch.setenv("HOME", str(empty))
        monkeypatch.setenv("XDG_CONFIG_HOME", str(empty))
    runtime._stop_signal.clear()
    yield
    runtime._stop_signal.clear()
    with runtime._shared_lock:
        shared = runtime._shared_session
    if shared is not None:
        shared.close()
