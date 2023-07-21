# Copyright © 2023 Pathway

from __future__ import annotations

import pytest
from utils import KafkaTestContext


@pytest.fixture
def kafka_context():
    kafka_context = KafkaTestContext()
    yield kafka_context
    kafka_context.teardown()
