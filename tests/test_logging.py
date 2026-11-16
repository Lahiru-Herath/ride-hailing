"""Tests for the shared structured (JSON) logger in observability/logging_config.py."""

import io
import json
import logging

from observability.logging_config import JsonFormatter


def make_logger(name: str):
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger(name)
    logger.handlers = [handler]
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger, stream


def test_log_line_is_one_json_object_with_the_standard_fields():
    logger, stream = make_logger("test_component_a")
    logger.info("something_happened", extra={"vehicle_id": "V001", "count": 3})
    record = json.loads(stream.getvalue())
    assert record["level"] == "INFO"
    assert record["component"] == "test_component_a"
    assert record["message"] == "something_happened"
    assert record["vehicle_id"] == "V001" and record["count"] == 3
    assert "timestamp" in record


def test_exceptions_are_serialised_into_the_same_json_line():
    logger, stream = make_logger("test_component_b")
    try:
        raise ValueError("boom")
    except ValueError:
        logger.exception("failed")
    record = json.loads(stream.getvalue())
    assert record["level"] == "ERROR" and "ValueError: boom" in record["exception"]