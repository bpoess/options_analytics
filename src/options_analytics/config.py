from __future__ import annotations

import tomllib
from pathlib import Path

from pydantic import AliasChoices, BaseModel, Field


class Account(BaseModel):
    id: str
    label: str = Field(validation_alias=AliasChoices("label", "name"))


class KeyConfig(BaseModel):
    api: str
    secret: str


class ETradeConfig(BaseModel):
    accounts: list[Account]
    key: KeyConfig

    def find_account_by_id(self, account_id: str) -> Account | None:
        for account in self.accounts:
            if account.id == account_id:
                return account


class Config(BaseModel):
    version: int
    etrade: ETradeConfig

    @staticmethod
    def from_file(path: str, overrides: list[str] | None = None) -> Config:
        if overrides is not None:
            raise Exception("Not implemented")

        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        with open(path, "rb") as f:
            data = tomllib.load(f)
            version = data.get("version")
            if version is None:
                data = convert_v0_to_v1_config(data)
                print(
                    "config.toml contains old-style configuration, "
                    "consider updating it to the newest schema."
                )
            elif version != 1:
                raise ValueError(f"Unsupported config version in {data['version']}")

            return Config.model_validate(data)


# Schema Converters


class AccountV0(BaseModel):
    id: str
    label: str = Field(validation_alias=AliasChoices("label", "name"))


class KeyConfigV0(BaseModel):
    api: str
    secret: str


class ETradeConfigV0(BaseModel):
    accounts: list[Account]
    key: KeyConfig


class UserConfigV0(BaseModel):
    name: str
    etrade: ETradeConfigV0


class ConfigV0(BaseModel):
    users: list[UserConfigV0]


def convert_v0_to_v1_config(data: dict) -> dict:
    config_v0 = ConfigV0.model_validate(data)
    if len(config_v0.users) > 1:
        print("config.toml needs manual changes. Only one user is supported now.")

    etrade_config_v0 = config_v0.users[0].etrade
    new_data = {}
    new_data["version"] = 1
    etrade = new_data["etrade"] = {}
    key = etrade["key"] = {}
    key["api"] = etrade_config_v0.key.api
    key["secret"] = etrade_config_v0.key.secret
    etrade["accounts"] = []
    for account_v0 in etrade_config_v0.accounts:
        account = {
            "id": account_v0.id,
            "label": account_v0.label,
        }
        etrade["accounts"].append(account)

    return new_data
