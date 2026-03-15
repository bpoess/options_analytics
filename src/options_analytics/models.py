from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal
from enum import Enum, StrEnum, auto

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TransactionCategory(StrEnum):
    NOT_SET = ""
    UNCATEGORIZED = "Uncategorized"
    # Labels should match spreadsheet labels
    ROLL = "Roll"
    EXPIRED = "Expired"
    CLOSED_EARLY = "Closed Early"
    ASSIGNED = "Assigned"
    OPEN = "Open"


class TransactionKind(Enum):
    SELL_OPEN = auto()
    BUY_CLOSE = auto()
    EXPIRED = auto()
    ASSIGNED = auto()


class CallOrPut(StrEnum):
    CALL = "CALL"
    PUT = "PUT"

    @staticmethod
    def from_str(call_or_put: str) -> CallOrPut:
        upper = call_or_put.upper()
        if upper == "CALL":
            return CallOrPut.CALL
        elif upper == "PUT":
            return CallOrPut.PUT
        else:
            raise ValueError(f"Unhandled call or put value '{call_or_put}'")


class OptionTransaction:
    id: str
    account_id: str
    account_label: str
    order_id: str | None = None
    date: datetime
    kind: TransactionKind
    quantity: Decimal
    price: Decimal
    fee: Decimal
    call_or_put: CallOrPut
    symbol: str
    expiry_date: str  # MMDDYY
    strike_price: Decimal

    # Meta data set during processing
    category: TransactionCategory = TransactionCategory.NOT_SET
    is_part_of_roll_order: bool = False

    @property
    def key(self):
        return (
            f"{self.symbol}{self.expiry_date}{self.call_or_put}{self.strike_price:f}"
            f"{self.account_label}"
        )

    def _format(self) -> str:
        return (
            f"Transaction({self.date.isoformat()} {self.account_label} "
            f"{self.key} category='{self.category}' kind={self.kind} "
            f"price={self.price:,f} "
            f"quantity={self.quantity:,f} "
            f"fee={self.fee:,f})"
        )

    def __str__(self) -> str:
        return self._format()

    def __repr__(self) -> str:
        return self._format()

    def format_for_script_output(self) -> str:
        bought_sold = "S" if self.kind == TransactionKind.SELL_OPEN else ""

        if self.category == TransactionCategory.OPEN:
            return (
                f"New Position\t{self.key}"
                f"\t{self.symbol}"
                f"\t{self.date.strftime('%m/%d/%y')}"
                f"\t{self.expiry_date}"
                f"\t{self.call_or_put}"
                f"\t{bought_sold}"
                f"\t{self.strike_price:,f}"
                f"\t{self.price:,f}"
                f"\t{self.quantity:,f}"
                f"\t{self.fee:,f}"
                f"\t{self.account_label}"
            )
        elif self.category == TransactionCategory.CLOSED_EARLY:
            return (
                f"Closed Early\t{self.key}"
                f"\t{self.quantity:,f}"
                f"\t{self.fee:,f}"
                f"\t{self.price:,f}"
                f"\t{self.date.strftime('%m/%d/%y')}"
            )
        elif self.category == TransactionCategory.ROLL:
            return (
                f"Roll\t{self.key}"
                f"\t{self.quantity:,f}"
                f"\t{self.fee:,f}"
                f"\t{self.price:,f}"
                f"\t{self.date.strftime('%m/%d/%y')}"
            )
        elif self.category == TransactionCategory.EXPIRED:
            return f"Expired\t{self.key}\t{self.quantity:,f}\t0\t0\t{self.expiry_date}"
        elif self.category == TransactionCategory.ASSIGNED:
            return (
                f"Assigned\t{self.key}"
                f"\t{self.quantity:,f}"
                f"\t0\t0"
                f"\t{self.date.strftime('%m/%d/%y')}"
            )
        else:
            raise Exception(f"Not implemented {self}")


class OptionQuote:
    date: datetime
    mark: Decimal
    intrinsic: Decimal
    days_to_expiration: int


class OptionPosition:
    id: str
    account_id: str
    account_label: str
    # Brokerage
    date_acquired: datetime
    cost_basis: Decimal
    commission: Decimal
    fees: Decimal
    # Product
    symbol: str
    call_or_put: CallOrPut
    expiry_year: int
    expiry_month: int
    expiry_day: int
    strike_price: Decimal
    quote: OptionQuote | None

    @property
    def quote_key(self) -> str:
        return (
            f"{self.symbol}:"
            f"{self.expiry_year:04d}:"
            f"{self.expiry_month:02d}:"
            f"{self.expiry_day:02d}:"
            f"{self.call_or_put}:"
            f"{self.strike_price:f}"
        )

    @property
    def expiry_date(self) -> str:
        return (
            f"{self.expiry_month:02d}/"
            f"{self.expiry_day:02d}/"
            f"{self.expiry_year % 100:02d}"
        )

    @property
    def product_key(self) -> str:
        return (
            f"{self.symbol}"
            f"{self.expiry_date}"
            f"{self.call_or_put}"
            f"{self.strike_price:f}"
            f"{self.account_label}"
        )

    @property
    def extrinsic(self) -> Decimal:
        assert self.quote
        return max(self.quote.mark - self.quote.intrinsic, Decimal("0"))

    @property
    def gain_percent(self) -> Decimal:
        assert self.quote
        gain = max(self.cost_basis - self.quote.mark, 0) / self.cost_basis
        return gain * Decimal("100")

    @property
    def remaining_annualized(self) -> Decimal:
        assert self.quote
        remaining = min(self.quote.mark, self.cost_basis)
        return (remaining / (self.strike_price * self.quote.days_to_expiration)) * 36500
