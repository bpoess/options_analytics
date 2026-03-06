import logging

from options_analytics.clients.etrade.session import ETradeSession

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class ETradeClient:
    """High-level E*Trade API client."""

    def __init__(
        self,
        consumer_key: str,
        consumer_secret: str,
        sandbox: bool = False,
    ):
        self._session = ETradeSession(consumer_key, consumer_secret, sandbox=sandbox)

    @property
    def session(self):
        """Get the underlying OAuth session."""
        return self._session.get_session()

    @property
    def base_url(self) -> str:
        """Get the API base URL."""
        return self._session.base_url

    def fetch_accounts(self) -> list[dict]:
        """Fetch and return the list of accounts."""
        url = f"{self.base_url}/v1/accounts/list"
        headers = {"Accept": "application/json"}

        response = self.session.get(url, headers=headers)
        response.raise_for_status()

        accounts_data = response.json()["AccountListResponse"]["Accounts"]["Account"]
        return accounts_data

    def fetch_order_list(
        self,
        account_id_key: str,
        start_date: str,
        end_date: str,
        status: str | None = None,
    ) -> list[dict]:
        """List orders for an account.

        Args:
            account_id_key: The account ID key for API calls
            start_date: Start date in MMDDYYYY format
            end_date: End date in MMDDYYYY format
            status: Order status filter

        Returns:
            List of Orders from the orders API
        """
        result = []

        url = f"{self.base_url}/v1/accounts/{account_id_key}/orders"
        headers = {"Accept": "application/json"}
        params = {"fromDate": start_date, "toDate": end_date, "status": status}
        response = self.session.get(url, headers=headers, params=params)
        if response.status_code != 200:
            logger.warning(
                f"No orders from account {account_id_key}, "
                f"status: {response.status_code}"
            )
            return result

        data = response.json()
        orders_response = data.get("OrdersResponse")
        response_order_list = orders_response.get("Order")
        logger.info(f"Received {len(response_order_list)} order summaries")

        result.extend(response_order_list)

        # Handle pagination using marker
        while orders_response.get("marker"):
            marker = orders_response["marker"]
            params["marker"] = marker
            response = self.session.get(url, headers=headers, params=params)
            response.raise_for_status()

            data = response.json()
            orders_response = data.get("OrdersResponse")
            response_order_list = orders_response.get("Order")
            logger.info(f"Received {len(response_order_list)} more order summaries")
            result.extend(response_order_list)

        return result

    def fetch_order_details(self, order: dict) -> dict | None:
        """Fetch details for a single order and return raw response."""
        headers = {"Accept": "application/json"}
        details_url = order["details"]

        response = self.session.get(details_url, headers=headers)
        response.raise_for_status()
        return response.json().get("OrdersResponse")

    def fetch_transactions(
        self,
        account_id_key: str,
        start_date: str,
        end_date: str,
    ) -> list:
        """Fetch transaction list for an account.

        Args:
            account_id_key: The account ID key for API calls
            start_date: Start date in MMDDYYYY format
            end_date: End date in MMDDYYYY format

        Returns:
            List of transactions
        """
        result = []

        url = f"{self.base_url}/v1/accounts/{account_id_key}/transactions"
        headers = {"Accept": "application/json"}
        params = {"fromDate": start_date, "toDate": end_date}
        response = self.session.get(url, headers=headers, params=params)
        if response.status_code != 200:
            logger.warning(
                f"No transactions from account {account_id_key}, "
                f"status: {response.status_code}"
            )
            return result

        data = response.json()
        transaction_list_response = data["TransactionListResponse"]
        transactions = transaction_list_response["Transaction"]
        logger.info(f"Received {len(transactions)} transactions")

        result.extend(transactions)

        # Handle pagination using marker
        while transaction_list_response.get("next"):
            response = self.session.get(
                transaction_list_response.get("next"), headers=headers
            )
            response.raise_for_status()

            data = response.json()

            transaction_list_response = data["TransactionListResponse"]
            transactions = transaction_list_response["Transaction"]
            logger.info(f"Received {len(transactions)} transactions")

            result.extend(transactions)

        return result

    def fetch_transaction_details(
        self, account_id_key: str, transactionId: str
    ) -> dict:
        """Fetch details the given transaction Id."""
        headers = {"Accept": "application/json"}

        logger.debug(f"Fetching details for {transactionId}")

        url = (
            f"{self.base_url}/v1/accounts/{account_id_key}/transactions/{transactionId}"
        )
        response = self.session.get(url, headers=headers)
        response.raise_for_status()

        return response.json().get("TransactionDetailsResponse")
