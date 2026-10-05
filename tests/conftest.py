from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqs_dlq_watch.testing import clients_from_spec, load_spec  # noqa: E402

FIXTURE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "examples",
    "fixtures",
    "queues.json",
)


@pytest.fixture()
def spec():
    return load_spec(FIXTURE)


@pytest.fixture()
def clients(spec):
    return clients_from_spec(spec)
