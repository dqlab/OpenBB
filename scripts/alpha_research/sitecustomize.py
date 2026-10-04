"""Test/build isolation only: prevent OpenBB from using real user settings.

Enable with PYTHONPATH=scripts/alpha_research. This changes configuration paths,
not HOME, discovery, providers or model registries. Subprocess builds inherit it.
"""

import os
from pathlib import Path

if os.environ.get("OPENBB_ALPHA_TEST_SETTINGS"):
    from openbb_core.app import constants

    root = Path(os.environ["OPENBB_ALPHA_TEST_SETTINGS"])
    root.mkdir(parents=True, exist_ok=True)
    constants.HOME_DIRECTORY = root
    constants.OPENBB_DIRECTORY = root / ".openbb_platform"
    constants.USER_SETTINGS_PATH = constants.OPENBB_DIRECTORY / "user_settings.json"
    constants.SYSTEM_SETTINGS_PATH = constants.OPENBB_DIRECTORY / "system_settings.json"
