from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import (
    AliasChoices,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)
from pydantic.alias_generators import to_camel

TransactionId = Annotated[str, BeforeValidator(lambda v: str(v))]
DateTimeFromMs = Annotated[
    datetime, BeforeValidator(lambda v: datetime.fromtimestamp(v / 1000.0))
]
DecimalAmount = Annotated[
    Decimal, BeforeValidator(lambda v: Decimal(str(v)).normalize())
]


class Account(BaseModel):
    """
    Source
    https://apisb.etrade.com/docs/api/account/api-account-v1.html#/definitions/Account
    """

    model_config = ConfigDict(alias_generator=to_camel, strict=True)

    inst_no: int | None = None
    id: str = Field(alias="accountId")
    id_key: str = Field(alias="accountIdKey")  # Unique account key
    mode: str = Field(alias="accountMode")
    desk: str | None = Field(default=None, alias="accountDesk")
    name: str = Field(alias="accountName")
    type: str = Field(alias="accountType")
    institution_type: str
    status: str = Field(alias="accountStatus")
    closed_date: DateTimeFromMs
    share_works_account: bool
    share_works_source: str | None = None
    fc_managed_mssb_closed_account: bool

    def __eq__(self, other):
        if not isinstance(other, Account):
            return False
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)


class ProductId(BaseModel):
    """
    Source
    https://apisb.etrade.com/docs/api/account/api-transaction-v1.html#/definitions/ProductId
    """

    model_config = ConfigDict(alias_generator=to_camel, strict=True)

    symbol: str
    type_code: str


class Product(BaseModel):
    """
    Source
    https://apisb.etrade.com/docs/api/account/api-transaction-v1.html#/definitions/Product
    """

    model_config = ConfigDict(alias_generator=to_camel, strict=True)

    symbol: str
    security_type: str | None = None
    security_sub_type: str | None = None
    # Below only set for options
    call_put: str | None = None
    expiry_year: int | None = None
    expiry_month: int | None = None
    expiry_day: int | None = None
    strike_price: DecimalAmount | None = None
    expiry_type: str | None = None
    product_id: ProductId | None = None

    @property
    def expiry_date(self) -> str:
        if (
            self.expiry_month is None
            or self.expiry_day is None
            or self.expiry_year is None
        ):
            return "(no expiration date set)"

        return (
            f"{self.expiry_month:02d}/{self.expiry_day:02d}/"
            f"{self.expiry_year % 100:02d}"
        )

    @property
    def key(self) -> str:
        if self.call_put is None or self.strike_price is None:
            return "[Unidentified]"

        return f"{self.symbol}{self.expiry_date}{self.call_put}{self.strike_price:f}"


class Brokerage(BaseModel):
    """
    Source
    https://apisb.etrade.com/docs/api/account/api-transaction-v1.html#/definitions/Brokerage
    """

    model_config = ConfigDict(alias_generator=to_camel, strict=True)

    transaction_type: str
    product: Product | None = Field(
        validation_alias=AliasChoices("Product", "product"), default=None
    )
    quantity: DecimalAmount
    price: DecimalAmount
    settlement_currency: str
    payment_currency: str
    fee: DecimalAmount
    order_no: str

    @field_validator("product", mode="before")
    @classmethod
    def empty_product_dict_to_none(cls, v: Any):
        # handles {}
        if v == {}:
            return None
        return v


class Transaction(BaseModel):
    """
    Source
    https://apisb.etrade.com/docs/api/account/api-transaction-v1.html#/definitions/TransactionDetailsResponse
    """

    model_config = ConfigDict(alias_generator=to_camel, strict=True)

    id: TransactionId = Field(alias="transactionId")
    account_id: str
    date: DateTimeFromMs = Field(alias="transactionDate")
    post_date: DateTimeFromMs | None = None
    amount: DecimalAmount
    description: str
    brokerage: Brokerage = Field(
        validation_alias=AliasChoices("Brokerage", "brokerage")
    )

    def __eq__(self, other):
        if not isinstance(other, Transaction):
            return False
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)


class Instrument(BaseModel):
    """
    Source
    https://apisb.etrade.com/docs/api/order/api-order-v1.html#/definitions/Instrument
    """

    model_config = ConfigDict(alias_generator=to_camel, strict=True)

    product: Product = Field(alias="Product")
    order_action: str
    ordered_quantity: DecimalAmount
    quantity_type: str
    average_execution_price: DecimalAmount | None = None
    estimated_commission: DecimalAmount
    estimated_fees: DecimalAmount
    filled_quantity: DecimalAmount
    symbol_description: str


class Event(BaseModel):
    """
    Source
    https://apisb.etrade.com/docs/api/order/api-order-v1.html#/definitions/Event
    """

    model_config = ConfigDict(alias_generator=to_camel, strict=True)

    name: str
    date_time: DateTimeFromMs
    instruments: list[Instrument] = Field(alias="Instrument")


class ExecutedOrder(BaseModel):
    """
    Source
    https://apisb.etrade.com/docs/api/order/api-order-v1.html#/definitions/Order
    """

    model_config = ConfigDict(alias_generator=to_camel, strict=True)

    id: str = Field(alias="orderId")
    type: str = Field(alias="orderType", default=None)
    executed_time: DateTimeFromMs
    status: str
    events: list[Event] = Field(alias="Events")

    @field_validator("id", mode="before")
    @classmethod
    def int_to_str(cls, v: Any):
        if isinstance(v, int):
            return str(v)
        return v

    @model_validator(mode="before")
    @classmethod
    def pre_process_data(cls, data: Any):
        if not isinstance(data, dict):
            return data

        # Flatten OrderDetail into the root dict
        orderDetail = data.get("OrderDetail")
        if orderDetail is None:
            raise KeyError("Expected 'OrderDetail'")

        if len(orderDetail) != 1:
            raise ValueError("Expected 'OrderDetail' to contain exactly one element")

        orderDetail = orderDetail[0]
        if isinstance(orderDetail, dict):
            data = dict(data)
            data.pop("OrderDetail", None)
            data.update(orderDetail)

        # Flatten Events from {Event:[{...}]} to [{...}]
        events = data.get("Events")
        if events is not None:
            events = events["Event"]
            data["Events"] = events

        return data

    def __eq__(self, other):
        if not isinstance(other, ExecutedOrder):
            return False
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)
