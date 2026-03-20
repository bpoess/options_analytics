from __future__ import annotations

import logging
from datetime import datetime, time, timedelta
from pprint import pformat
from typing import Any
from urllib.parse import urlsplit

from etrade_client.client import ETradeClient

logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)


def date_filter(transaction, start_date_time, end_date_time_exclusive):
    date = datetime.fromtimestamp(int(transaction["transactionDate"]) / 1000.0)
    logger.debug(
        f"Comparing {transaction['transactionDate']}, {date}, {start_date_time}, "
        f"{end_date_time_exclusive} result "
        f"{date >= start_date_time and date < end_date_time_exclusive}"
    )
    return date >= start_date_time and date < end_date_time_exclusive


class ETradeCachedClient:
    """Wrapper around ETradeClient that supports cached data."""

    _cached: bool
    _client: ETradeClient
    _json_data: dict[str, Any]

    def __init__(
        self,
        consumer_key: str,
        consumer_secret: str,
        cache_data: dict | None = None,
        sandbox: bool = False,
    ):
        if not cache_data:
            self._client = ETradeClient(consumer_key, consumer_secret, sandbox=sandbox)
            self._cached = False
        else:
            self._cached = True
            self._json_data = cache_data

    def is_authenticated(self) -> bool:
        if self._cached:
            return True
        return self._client.is_authenticated()

    def get_authorization_url(self) -> str:
        if self._cached:
            raise RuntimeError("Cached client does not require authorization")
        return self._client.get_authorization_url()

    def complete_authorization(self, verification_code: str) -> None:
        if self._cached:
            raise RuntimeError("Cached client does not require authorization")
        self._client.complete_authorization(verification_code)

    def fetch_accounts(self) -> list[dict[str, Any]]:
        if self._cached:
            return self._json_data["Accounts"]
        else:
            return self._client.fetch_accounts()

    def _get_cached_account(self, account_id_key: str) -> dict[str, Any]:
        for account in self._json_data["Accounts"]:
            if account_id_key == account["accountIdKey"]:
                return account

        raise ValueError(
            f"Cached account not found for account_id_key={account_id_key}"
        )

    def _fetch_cached_transactions(
        self,
        account_id_key: str,
        start_date_str: str,
        end_date_str: str,
    ) -> list:
        account = self._get_cached_account(account_id_key)
        transactions = self._json_data["Transactions"][account["accountId"]]

        start_date = datetime.strptime(start_date_str, "%m%d%Y").date()
        end_date = datetime.strptime(end_date_str, "%m%d%Y").date()

        start_date_time = datetime.combine(start_date, time.min)  # 00:00:00
        end_date_time_exclusive = datetime.combine(
            end_date + timedelta(days=1), time.min
        )

        return list(
            filter(
                lambda tx: date_filter(tx, start_date_time, end_date_time_exclusive),
                transactions,
            )
        )

    def fetch_transactions(
        self,
        account_id_key: str,
        start_date: str,
        end_date: str,
    ) -> list:
        if self._cached:
            transactions = self._fetch_cached_transactions(
                account_id_key, start_date, end_date
            )
            logger.debug(f"Found {len(transactions)} transactions")
            return transactions
        else:
            return self._client.fetch_transactions(account_id_key, start_date, end_date)

    def fetch_transaction_details(
        self,
        account_id_key: str,
        transactionId: str,
    ) -> dict:
        if self._cached:
            account = self._get_cached_account(account_id_key)
            logger.debug(f"Looking up {transactionId} in {account['accountId']}")
            logger.debug(
                pformat(
                    self._json_data["TransactionDetails"][account["accountId"]][
                        transactionId
                    ]
                )
            )
            return self._json_data["TransactionDetails"][account["accountId"]][
                transactionId
            ]
        else:
            return self._client.fetch_transaction_details(account_id_key, transactionId)

    def fetch_order_list(
        self,
        account_id_key: str,
        start_date_str: str,
        end_date_str: str,
        status: str | None = None,
    ) -> list[dict]:
        """List orders for an account.

        Args:
            account_id_key: The account ID key for API calls
            start_date: Start date in MMDDYYYY format
            end_date: End date in MMDDYYYY format
            status

        Returns:
            List of Orders from the orders API
        """
        if self._cached:
            account = self._get_cached_account(account_id_key)
            orders = self._json_data["OrderList"][account["accountId"]]

            start_date = datetime.strptime(start_date_str, "%m%d%Y").date()
            end_date = datetime.strptime(end_date_str, "%m%d%Y").date()

            start_date_time = datetime.combine(start_date, time.min)  # 00:00:00
            end_date_time_exclusive = datetime.combine(
                end_date + timedelta(days=1), time.min
            )

            result = []
            for order in orders:
                if len(order["OrderDetail"]) != 1:
                    logger.error(order)
                    raise ValueError("Expected exactly one element in OrderDetail")

                if status is not None:
                    if order["OrderDetail"][0]["status"] != status:
                        continue

                if status is None or status != "EXECUTED":
                    raise Exception(
                        "Listing non-executed orders is not supported at the moment"
                    )

                # Filter by start/end date
                date = datetime.fromtimestamp(
                    int(order["OrderDetail"][0]["executedTime"]) / 1000.0
                )
                if date < start_date_time or date >= end_date_time_exclusive:
                    continue

                result.append(order)

            return result

        else:
            return self._client.fetch_order_list(
                account_id_key, start_date_str, end_date_str, status
            )

    def fetch_order_details(self, order: dict):
        if self._cached:
            path_segments = urlsplit(order["details"]).path.strip("/").split("/")
            account_id_key = path_segments[path_segments.index("accounts") + 1]
            account = self._get_cached_account(account_id_key)
            return self._json_data["OrderDetails"][account["accountId"]].get(
                str(order["orderId"])
            )
        else:
            return self._client.fetch_order_details(order)
