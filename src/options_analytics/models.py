import logging
from enum import StrEnum

from tqdm import tqdm

from options_analytics.clients.etrade import models as etrade_models
from options_analytics.clients.etrade.cache_client import ETradeCachedClient

logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)


class TransactionCategory(StrEnum):
    NOT_SET = ""
    UNCATEGORIZED = "Uncategorized"
    # Labels should match spreadsheet labels
    ROLL = "Roll"
    EXPIRED = "Expired"
    CLOSED_EARLY = "Closed Early"
    ASSIGNED = "Assigned"
    OPEN = "Open"


class ExecutedOrder(etrade_models.ExecutedOrder):
    category: TransactionCategory = TransactionCategory.NOT_SET


class Transaction(etrade_models.Transaction):
    category: TransactionCategory = TransactionCategory.NOT_SET
    _order_category: TransactionCategory = TransactionCategory.NOT_SET
    _roll_id: str = ""
    _account_label: str = ""

    @property
    def key(self):
        return f"{self.brokerage.product.key}{self._account_label}"

    def _format(self) -> str:
        category = (
            self.category
            if self.category != TransactionCategory.NOT_SET
            else self.brokerage.transaction_type
        )
        return (
            f"Transaction({self.date.isoformat()} {self._account_label} "
            f"{self.key} category='{category}' price={self.brokerage.price:,f} "
            f"quantity={abs(self.brokerage.quantity):,f} "
            f"fee={self.brokerage.fee:,f})"
        )

    def __str__(self) -> str:
        return self._format()

    def __repr__(self) -> str:
        return self._format()

    def format_for_script_output(self) -> str:
        product = self.brokerage.product

        bought_sold = ""
        if self.brokerage.transaction_type == "Sold Short":
            bought_sold = "S"
        elif self.brokerage.transaction_type == "Bought To Open":
            bought_sold = "B"

        if self.category == TransactionCategory.OPEN:
            return (
                f"New Position\t{self.key}"
                f"\t{self.symbol}"
                f"\t{self.date.strftime('%m/%d/%y')}"
                f"\t{product.expiry_date}"
                f"\t{product.call_put.capitalize()}"
                f"\t{bought_sold}"
                f"\t{product.strike_price:,f}"
                f"\t{self.brokerage.price:,f}"
                f"\t{abs(self.brokerage.quantity):,f}"
                f"\t{self.brokerage.fee:,f}"
                f"\t{self._account_label}"
            )
        elif self.category == TransactionCategory.CLOSED_EARLY:
            return (
                f"Closed Early\t{self.key}"
                f"\t{abs(self.brokerage.quantity):,f}"
                f"\t{self.brokerage.fee:,f}"
                f"\t{self.brokerage.price:,f}"
                f"\t{self.date.strftime('%m/%d/%y')}"
            )
        elif self.category == TransactionCategory.ROLL:
            return (
                f"Roll\t{self.key}"
                f"\t{abs(self.brokerage.quantity):,f}"
                f"\t{self.brokerage.fee:,f}"
                f"\t{self.brokerage.price:,f}"
                f"\t{self.date.strftime('%m/%d/%y')}"
            )
        elif self.category == TransactionCategory.EXPIRED:
            return (
                f"Expired\t{self.key}"
                f"\t{abs(self.brokerage.quantity):,f}"
                f"\t0\t0"
                f"\t{product.expiry_date}"
            )
        elif self.category == TransactionCategory.ASSIGNED:
            return (
                f"Assigned\t{self.key}"
                f"\t{abs(self.brokerage.quantity):,f}"
                f"\t0\t0"
                f"\t{self.date.strftime('%m/%d/%y')}"
            )
        else:
            raise Exception(f"Not implemented {self}")


class Account(etrade_models.Account):
    _orders: dict[ExecutedOrder] = dict()
    _transactions: dict[Transaction] = dict()
    _configured_name: str

    def fetch_orders(self, client: ETradeCachedClient, startdate: str, enddate: str):
        with tqdm(
            total=0, desc=f"Fetching orders for {self.id}", leave=True
        ) as progress_bar:
            order_dicts = client.fetch_order_list(
                self.id_key, startdate, enddate, "EXECUTED"
            )
            progress_bar.total += len(order_dicts)

            for order_dict in order_dicts:
                detailed_json = client.fetch_order_details(order_dict)
                try:
                    if len(detailed_json["Order"]) != 1:
                        raise ValueError(
                            "Expected detailed order response list to contain "
                            "exactly one element"
                        )
                    order = ExecutedOrder.model_validate(detailed_json["Order"][0])
                    self._orders[order.id] = order
                    progress_bar.update(1)

                except Exception as e:
                    logger.error(order_dict)
                    logger.error(detailed_json)
                    raise e

        logger.debug(f"Account {self.id}: Fetched {len(self._orders)} orders")

    @property
    def transactions(self):
        return [
            transaction
            for _unused, transaction in self._transactions.items()
            if not Account.transaction_filter(transaction)
        ]

    def fetch_transactions(
        self, client: ETradeCachedClient, startdate: str, enddate: str
    ) -> list[etrade_models.Transaction]:
        with tqdm(
            total=0, desc=f"Fetching transactions for {self.id}", leave=True
        ) as progress_bar:
            transactions = client.fetch_transactions(self.id_key, startdate, enddate)
            progress_bar.total += len(transactions)
            for data in transactions:
                detailed_data = client.fetch_transaction_details(
                    self.id_key, data["transactionId"]
                )
                transaction = Transaction.model_validate(detailed_data)
                transaction._account_label = self._configured_name
                self._transactions[transaction.id] = transaction
                progress_bar.update(1)

            logger.debug(f"Fetched {len(self._transactions)} transactions")

    @staticmethod
    def transaction_filter(transaction):
        TRANSACTION_TYPE_TO_SKIP = {
            "Bought",
            "Sold",
            "MISC",
            "Type",
            "Interest",
            "Sweep",
            "Receipt",
            "Transfer",
            "Adjustment",
            "Journal",
            "Dividend",
            "LT Cap Gain Distribution",
            "Interest Income",
            "Online Transfer",
            "Margin Interest",
            "Misc Income",
            "Wire In",
            "Qualified Dividend",
            "Funds Received",
        }
        return transaction.brokerage.transaction_type in TRANSACTION_TYPE_TO_SKIP

    def classify_order(self, order_id: str) -> TransactionCategory:
        order = self._orders.get(order_id)
        if order is None:
            logger.debug(f"Did not find order {order_id}")
            return TransactionCategory.UNCATEGORIZED

        sell_open = 0
        buy_close = 0
        for event in order.events:
            if event.name == "ORDER_EXECUTED":
                for instrument in event.instruments:
                    if instrument.product.security_type == "OPTN":
                        if instrument.order_action == "SELL_OPEN":
                            sell_open += 1
                        elif instrument.order_action == "BUY_CLOSE":
                            buy_close += 1
                        else:
                            logger.debug(
                                f"Order {order_id} Unknown order action "
                                f"{instrument.order_action}"
                            )
                            return TransactionCategory.UNCATEGORIZED

        if sell_open > 0 and buy_close == 0:
            return TransactionCategory.OPEN
        elif sell_open == 0 and buy_close > 0:
            return TransactionCategory.CLOSED_EARLY
        elif sell_open > 0 and buy_close > 0:
            return TransactionCategory.ROLL

        logger.debug(
            f"Order {order_id} unhandled state sell_open {sell_open} "
            f"buy_close {buy_close}"
        )
        return TransactionCategory.UNCATEGORIZED

    def classify_transaction(self, transaction):
        """Set the transaction's category and its order category where applicable"""

        if (
            transaction.brokerage.order_no is not None
            and int(transaction.brokerage.order_no) != 0
        ):
            transaction._order_category = self.classify_order(
                transaction.brokerage.order_no
            )

        if transaction.brokerage.transaction_type in ["Sold Short", "Bought To Open"]:
            transaction.category = TransactionCategory.OPEN
            return
        elif transaction.brokerage.transaction_type == "Option Expired":
            transaction.category = TransactionCategory.EXPIRED
            return
        elif transaction.brokerage.transaction_type == "Option Assigned":
            transaction.category = TransactionCategory.ASSIGNED
            return

        # Not opened, expired, or assigned. Try to classify roll or closed early
        if transaction._order_category != TransactionCategory.UNCATEGORIZED:
            transaction.category = transaction._order_category
            return

        logger.debug(f"Transaction {transaction.id} Unable to classify")
        return TransactionCategory.UNCATEGORIZED
