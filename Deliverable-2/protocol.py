"""Shared newline-delimited JSON protocol helpers."""

from __future__ import annotations

import json
from typing import Any, Iterator


MAX_MESSAGE_BYTES = 64 * 1024


def encode_message(message: dict[str, Any]) -> bytes:
    """Encode one protocol message as a compact newline-delimited JSON frame."""
    return (json.dumps(message, separators=(",", ":"), sort_keys=True) + "\n").encode(
        "utf-8"
    )


def read_messages(buffer: bytes, chunk: bytes) -> tuple[bytes, list[dict[str, Any]]]:
    """Append a TCP chunk and return complete JSON messages plus the remaining bytes."""
    buffer += chunk
    if len(buffer) > MAX_MESSAGE_BYTES:
        raise ValueError("protocol buffer exceeded maximum size")

    messages: list[dict[str, Any]] = []
    while b"\n" in buffer:
        raw, buffer = buffer.split(b"\n", 1)
        if not raw:
            continue
        message = json.loads(raw.decode("utf-8"))
        if not isinstance(message, dict):
            raise ValueError("protocol message must be a JSON object")
        messages.append(message)
    return buffer, messages


def validate_string(message: dict[str, Any], field: str, maximum: int = 256) -> str:
    """Validate a required bounded string field."""
    value = message.get(field)
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError(f"{field} must be a non-empty string of at most {maximum} characters")
    return value.strip()
