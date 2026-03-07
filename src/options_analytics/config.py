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


class UserConfig(BaseModel):
    name: str
    etrade: ETradeConfig


class Config(BaseModel):
    users: list[UserConfig]

    @staticmethod
    def from_file(path: str, overrides: list[str] = None) -> Config:
        if overrides is not None:
            raise Exception("Not implemented")

        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        with open(path, "rb") as f:
            data = tomllib.load(f)
            return Config.model_validate(data)
