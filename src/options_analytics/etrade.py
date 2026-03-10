import logging

from tqdm import tqdm

from options_analytics.clients.etrade import models as etrade_models
from options_analytics.clients.etrade.cache_client import ETradeCachedClient
from options_analytics.config import ETradeConfig
from options_analytics.models import CallOrPut, OptionTransaction, TransactionKind

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Account:
    id: str
    id_key: str
    label: str  # Configured label for this account

    def __init__(self, id: str, id_key: str, label: str):
        self.id = id
        self.id_key = id_key
        self.label = label


class Repository:
    _config: ETradeConfig
    _client: ETradeCachedClient
    _accounts: list[Account] = []

    def __init__(self, config: ETradeConfig, json_data: dict | None = None):
        self._config = config
        self._client = ETradeCachedClient(
            self._config.key.api,
            self._config.key.secret,
            json_data,
        )

    def _fetch_accounts(self):
        accounts = self._client.fetch_accounts()

        for data in accounts:
            etrade_account = etrade_models.Account.model_validate(data)
            configured_account = self._config.find_account_by_id(etrade_account.id)
            if not configured_account:
                logger.debug(f"Not configured {etrade_account}, ignoring")
                continue

            self._accounts.append(
                Account(
                    etrade_account.id, etrade_account.id_key, configured_account.label
                )
            )

    @staticmethod
    def option_transaction_filter(transaction):
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
        return transaction.brokerage.transaction_type not in TRANSACTION_TYPE_TO_SKIP

    def _fetch_option_transactions_for_account(
        self,
        account: Account,
        startdate: str,
        enddate: str,
        option_transactions: dict[OptionTransaction],
    ):
        with tqdm(
            total=0, desc=f"Fetching transactions for {account.label}", leave=True
        ) as progress_bar:
            transaction_list = self._client.fetch_transactions(
                account.id_key, startdate, enddate
            )
            progress_bar.total += len(transaction_list)
            for data in transaction_list:
                detailed_data = self._client.fetch_transaction_details(
                    account.id_key, data["transactionId"]
                )
                transaction = etrade_models.Transaction.model_validate(detailed_data)

                if Repository.option_transaction_filter(transaction):
                    # Sanity checks
                    if (
                        not transaction.brokerage.product.call_put
                        or transaction.brokerage.settlement_currency != "USD"
                        or transaction.brokerage.payment_currency != "USD"
                    ):
                        raise ValueError(f"Unexpected values in {transaction}")

                    option_transaction = OptionTransaction()
                    option_transaction.id = transaction.id
                    option_transaction.account_id = transaction.account_id
                    option_transaction.account_label = account.label
                    if (
                        transaction.brokerage.order_no is not None
                        and transaction.brokerage.order_no != ""
                        and int(transaction.brokerage.order_no) != 0
                    ):
                        option_transaction.order_id = transaction.brokerage.order_no
                    option_transaction.date = transaction.date
                    match transaction.brokerage.transaction_type:
                        case "Sold Short":
                            option_transaction.kind = TransactionKind.SELL_OPEN
                        case "Bought To Cover":
                            option_transaction.kind = TransactionKind.BUY_CLOSE
                        case "Option Expired":
                            option_transaction.kind = TransactionKind.EXPIRED
                        case "Option Assigned":
                            option_transaction.kind = TransactionKind.ASSIGNED
                        case _:
                            raise ValueError(
                                f"Unexpected transaction type "
                                f"{transaction.brokerage.transaction_type} "
                                f"{transaction}"
                            )
                    option_transaction.quantity = abs(transaction.brokerage.quantity)
                    option_transaction.price = transaction.brokerage.price
                    option_transaction.fee = transaction.brokerage.fee
                    match transaction.brokerage.product.call_put:
                        case "PUT":
                            option_transaction.call_or_put = CallOrPut.PUT
                        case "CALL":
                            option_transaction.call_or_put = CallOrPut.CALL
                        case _:
                            raise ValueError(f"Unexpected call_put value {transaction}")
                    option_transaction.symbol = transaction.brokerage.product.symbol
                    option_transaction.expiry_date = (
                        transaction.brokerage.product.expiry_date
                    )
                    option_transaction.strike_price = (
                        transaction.brokerage.product.strike_price
                    )

                    option_transactions[transaction.id] = option_transaction
                    logger.debug(f"Adding {option_transaction.id} {option_transaction}")

                progress_bar.update(1)

    def list_option_transactions(
        self, startdate: str, enddate: str
    ) -> list[OptionTransaction]:
        if not self._accounts:
            self._fetch_accounts()

        option_transactions = {}
        for account in self._accounts:
            self._fetch_option_transactions_for_account(
                account, startdate, enddate, option_transactions
            )

        transactions = [
            transaction for _unused, transaction in option_transactions.items()
        ]
        transactions.sort(key=lambda t: (t.date, t.id))
        logger.debug(
            f"Returning {len(transactions)} transactions, {len(option_transactions)}"
        )
        return transactions
