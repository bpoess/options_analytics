import configparser
import json
from pathlib import Path

from pydantic import BaseModel

_current_config: configparser.ConfigParser | None = None


class Account(BaseModel):
    id: str
    name: str

    def __eq__(self, other):
        if not isinstance(other, Account):
            return False
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)


def initialize(path: str | Path = "config.ini"):
    """Initialize the application config."""
    global _current_config
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    _current_config = configparser.ConfigParser()
    _current_config.read(path)


def _get() -> configparser.ConfigParser:
    """Get the current config. Must call initialize() first."""
    if _current_config is None:
        raise RuntimeError("Config not initialized. Call config.initialize() first.")
    return _current_config


def account_ids() -> list[str]:
    return [
        account.id
        for account in map(
            lambda data: Account.model_validate(data),
            json.loads(_get()["DEFAULT"]["ACCOUNT_LIST"]),
        )
    ]


def accounts() -> list[Account]:
    return list(
        map(
            lambda data: Account.model_validate(data),
            json.loads(_get()["DEFAULT"]["ACCOUNT_LIST"]),
        )
    )


def etrade_consumer_key() -> str:
    return _get()["DEFAULT"]["CONSUMER_KEY"]


def etrade_consumer_secret() -> str:
    return _get()["DEFAULT"]["CONSUMER_SECRET"]


def reset():
    """Reset config state. Useful for testing."""
    global _current_config
    _current_config = None
