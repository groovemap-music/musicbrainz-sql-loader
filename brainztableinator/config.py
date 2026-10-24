"""MusicBrainz SQL loader configuration owned by this service."""

from dataclasses import dataclass, field
from os import getenv
from typing import cast

from common.config import (
    _build_amqp_url,
    _build_postgres_connstr,
    get_secret,
    resolve_postgres_pool_sizes,
)


DEFAULT_HEALTH_PORT = 8002


def _resolve_health_port() -> int:
    """Read HEALTH_PORT, defaulting to the SQL loader role port; reject invalid values."""
    raw = getenv("HEALTH_PORT")
    if raw is None:
        return DEFAULT_HEALTH_PORT
    try:
        port = int(raw.strip())
    except ValueError:
        raise ValueError(f"HEALTH_PORT must be an integer between 1 and 65535, got {raw!r}") from None
    if not 1 <= port <= 65535:
        raise ValueError(f"HEALTH_PORT must be an integer between 1 and 65535, got {raw!r}")
    return port


@dataclass(frozen=True)
class MusicBrainzSQLLoaderConfig:
    """Configuration for the MusicBrainz SQL loader."""

    amqp_connection: str = field(repr=False)
    postgres_host: str
    postgres_username: str
    postgres_password: str = field(repr=False)
    postgres_database: str
    postgres_pool_min_size: int = 2
    postgres_pool_max_size: int = 12
    health_port: int = DEFAULT_HEALTH_PORT

    @classmethod
    def from_env(cls) -> MusicBrainzSQLLoaderConfig:
        """Create configuration from environment variables."""
        rabbitmq_username = get_secret("RABBITMQ_USERNAME")
        rabbitmq_password = get_secret("RABBITMQ_PASSWORD")
        postgres_username = get_secret("POSTGRES_USERNAME")
        postgres_password = get_secret("POSTGRES_PASSWORD")
        postgres_database = getenv("POSTGRES_DATABASE")
        missing_vars = [
            name
            for name, value in (
                ("RABBITMQ_USERNAME", rabbitmq_username),
                ("RABBITMQ_PASSWORD", rabbitmq_password),
                ("POSTGRES_HOST", getenv("POSTGRES_HOST")),
                ("POSTGRES_USERNAME", postgres_username),
                ("POSTGRES_PASSWORD", postgres_password),
                ("POSTGRES_DATABASE", postgres_database),
            )
            if not value
        ]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        health_port = _resolve_health_port()
        pool_min, pool_max = resolve_postgres_pool_sizes(default_min=2, default_max=12)
        return cls(
            amqp_connection=_build_amqp_url(),
            postgres_host=_build_postgres_connstr(),
            postgres_username=cast("str", postgres_username),
            postgres_password=cast("str", postgres_password),
            postgres_database=cast("str", postgres_database),
            postgres_pool_min_size=pool_min,
            postgres_pool_max_size=pool_max,
            health_port=health_port,
        )


# Compatibility alias for callers that imported the pre-extraction class name.
BrainztableinatorConfig = MusicBrainzSQLLoaderConfig
