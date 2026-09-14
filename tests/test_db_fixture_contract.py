"""Contract checks for the shared PostgreSQL test boundary."""

import inspect
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_postgres_fixture_chain_matches_runtime_protocol(
    mock_async_pool: MagicMock,
    mock_connection: MagicMock,
    mock_cursor: MagicMock,
    mock_transaction: MagicMock,
) -> None:
    """Factories stay synchronous while I/O and context entry remain awaitable."""
    assert not inspect.isawaitable(mock_async_pool.connection())
    assert not inspect.isawaitable(mock_connection.cursor())
    assert not inspect.isawaitable(mock_connection.transaction())

    async with mock_async_pool.connection() as connection:
        async with connection.cursor() as cursor:
            await cursor.execute("SELECT 1")
        async with connection.transaction() as transaction:
            assert transaction is mock_transaction

    assert connection is mock_connection
    assert cursor is mock_cursor
    assert isinstance(mock_async_pool.close, AsyncMock)
    assert isinstance(mock_cursor.execute, AsyncMock)
    mock_cursor.execute.assert_awaited_once_with("SELECT 1")

    for boundary in (mock_async_pool, mock_connection, mock_cursor, mock_transaction):
        with pytest.raises(AttributeError):
            _missing_runtime_method = boundary.not_a_runtime_method
