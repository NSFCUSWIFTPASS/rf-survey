import sys
from unittest.mock import MagicMock

import pytest


def pytest_configure(config):
    uhd_mock = MagicMock()
    sys.modules["uhd"] = uhd_mock
