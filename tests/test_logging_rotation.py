"""Proves this service's file log sink rotates instead of growing unbounded.

Every other test in this suite mocks ``setup_logging`` at the call site, so nothing else
exercises the real handler construction for this service's own ``log_file`` argument. This
is the failure mode that once grew one service's ``/logs`` file to 675 GB and filled 96% of
a host disk: a log_file handler that never rolls over. ``groovemap-runtime`` now builds a
size-capped ``RotatingFileHandler`` for any consumer that passes ``log_file=...`` to
``setup_logging`` (see ``common.log_rotation.build_rotating_file_handler``, covered by that
package's own ``tests/test_log_rotation.py``); this test proves the call this service
actually makes goes through that same bounded path, using the exact ``log_file`` shape
``main()`` uses (``Path(f"/logs/{SERVICE_NAME}.log")``).
"""

from __future__ import annotations

from logging.handlers import RotatingFileHandler
from typing import TYPE_CHECKING
from unittest.mock import patch

from common import setup_logging
from common.log_rotation import DEFAULT_LOG_FILE_BACKUP_COUNT, DEFAULT_LOG_FILE_MAX_BYTES

from brainztableinator.brainztableinator import SERVICE_NAME


if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_service_log_file_handler_is_size_capped_rotating_handler(tmp_path: Path) -> None:
    """setup_logging(SERVICE_NAME, log_file=...) binds a RotatingFileHandler.

    ``logging.basicConfig`` is patched only so this test does not clobber the real root
    logger for the rest of the suite; ``build_rotating_file_handler`` itself runs for real,
    so the assertions below are against the actual handler the service would install.
    """
    log_file = tmp_path / f"{SERVICE_NAME}.log"

    with patch("common.config.logging.basicConfig") as basic_config:
        setup_logging(SERVICE_NAME, log_file=log_file)

    handlers = basic_config.call_args.kwargs["handlers"]
    rotating_handlers = [h for h in handlers if isinstance(h, RotatingFileHandler)]
    assert len(rotating_handlers) == 1, f"expected exactly one rotating file handler, got {handlers!r}"

    handler = rotating_handlers[0]
    try:
        assert handler.baseFilename == str(log_file)
        assert handler.maxBytes == DEFAULT_LOG_FILE_MAX_BYTES
        assert handler.backupCount == DEFAULT_LOG_FILE_BACKUP_COUNT
    finally:
        handler.close()


def test_service_log_file_handler_honors_env_overrides(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A deployment can tune the cap and backup count without a rebuild."""
    monkeypatch.setenv("LOG_FILE_MAX_BYTES", "2048")
    monkeypatch.setenv("LOG_FILE_BACKUP_COUNT", "3")
    log_file = tmp_path / f"{SERVICE_NAME}.log"

    with patch("common.config.logging.basicConfig") as basic_config:
        setup_logging(SERVICE_NAME, log_file=log_file)

    handler = next(h for h in basic_config.call_args.kwargs["handlers"] if isinstance(h, RotatingFileHandler))
    try:
        assert handler.maxBytes == 2048
        assert handler.backupCount == 3
    finally:
        handler.close()
