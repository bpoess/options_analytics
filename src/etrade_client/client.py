import json
import logging
import os
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from pprint import pformat
from typing import Any
from urllib.parse import parse_qsl, urlencode

import requests
from oauthlib.oauth1 import Client as OAuth1Client
from pytz import timezone

from etrade_client.exceptions import AuthenticationRequired

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

PROD_BASE_URL = "https://api.etrade.com"
SANDBOX_BASE_URL = "https://apisb.etrade.com"
AUTH_TOKEN_URL = "https://us.etrade.com/e/t/etws/authorize"

# Re-export for backwards compatibility
__all__ = ["AuthenticationRequired", "ETradeClient"]


class ETradeClient:
    """E*Trade API client with integrated OAuth1 session management."""

    _oauth_client: OAuth1Client | None = None

    def __init__(
        self,
        consumer_key: str,
        consumer_secret: str,
        token_path: str | Path = "access_token.json",
        sandbox: bool = False,
    ):
        self._consumer_key = consumer_key
        self._consumer_secret = consumer_secret
        self._token_path = Path(token_path)
        self._sandbox = sandbox
        self._http = requests.Session()
        self._pending_request_token: dict | None = None
        self._token_mtime: float | None = None

    @property
    def _base_url(self) -> str:
        """Get the API base URL."""
        return SANDBOX_BASE_URL if self._sandbox else PROD_BASE_URL

    # -- OAuth session management --

    def validate_session(self) -> None:
        """Validate OAuth session eagerly (e.g. at startup).

        Raises AuthenticationRequired if no valid session exists.
        """
        if not self.is_authenticated():
            raise AuthenticationRequired("No valid OAuth session")

    def _get(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        params: Mapping[str, str | None] | None = None,
        _retry: bool = True,
    ) -> requests.Response:
        """Sign and execute a GET request.

        Raises AuthenticationRequired if the client is not authenticated.
        On 401/403, invalidates the session and retries once if a valid
        token can be reloaded (e.g. written by an external process).
        """
        if self._oauth_client is None:
            if not self.is_authenticated():
                raise AuthenticationRequired("Client is not authenticated")
        assert self._oauth_client is not None

        # Encode non-None params into the URL for correct OAuth signing
        if params:
            filtered = {k: v for k, v in params.items() if v is not None}
            if filtered:
                url = f"{url}?{urlencode(filtered)}"

        signed_url, signed_headers, _ = self._oauth_client.sign(
            url, http_method="GET", headers=headers
        )
        response = self._http.get(signed_url, headers=signed_headers)
        if response.status_code != 200:
            try:
                logger.warning(f"API returned {response.status_code} {response.json()}")
            except Exception:
                logger.warning(f"API returned {response.status_code} {response.text}")
                pass
        if response.status_code in (401, 403):
            logger.info(f"API returned {response.status_code}, invalidating session")
            self._invalidate()
            if _retry and self.is_authenticated():
                logger.info("Re-authenticated, retrying request")
                return self._get(url, headers=headers, _retry=False)
            raise AuthenticationRequired(
                f"API returned {response.status_code}, session is no longer valid"
            )
        return response

    def _token_changed_on_disk(self) -> bool:
        """Check if the token file has been modified since we last loaded it."""
        if not self._token_path.exists():
            return self._token_mtime is not None
        try:
            return self._token_path.stat().st_mtime != self._token_mtime
        except OSError:
            return False

    def _load_cached_token(self) -> dict | None:
        """Load token from disk if it exists."""
        if not self._token_path.exists():
            return None
        with open(self._token_path) as f:
            token = json.load(f)
        self._token_mtime = self._token_path.stat().st_mtime
        return token

    def _save_token(self, token: dict) -> None:
        """Save token to disk with restricted permissions."""
        with open(self._token_path, "w") as f:
            json.dump(token, f)
        os.chmod(self._token_path, 0o600)
        self._token_mtime = self._token_path.stat().st_mtime

    def _remove_token(self) -> None:
        """Remove cached token file if it exists."""
        if self._token_path.exists():
            os.remove(self._token_path)

    def _is_token_expired(self) -> bool:
        """Check if token has expired (expires at midnight US Eastern)."""
        if not self._token_path.exists():
            return False
        tz = timezone("US/Eastern")
        mod_time = datetime.fromtimestamp(self._token_path.stat().st_mtime, tz)
        today_time = datetime.now(tz)
        return today_time.date() != mod_time.date()

    def _renew_session(self) -> bool:
        """Renew the session token. Returns True on success, False on failure."""
        assert self._oauth_client is not None

        try:
            url = f"{self._base_url}/oauth/renew_access_token"
            signed_url, signed_headers, _ = self._oauth_client.sign(
                url, http_method="GET"
            )
            self._http.get(signed_url, headers=signed_headers)
            return True
        except Exception as e:
            logger.info(f"Unable to renew session, removing cached token. {e}")
            self._invalidate()
            return False

    def _invalidate(self) -> None:
        """Clear the OAuth client and remove the cached token."""
        logger.info("Invalidating OAuth session")
        self._oauth_client = None
        if self._token_mtime is not None and self._token_path.exists():
            try:
                if self._token_path.stat().st_mtime == self._token_mtime:
                    os.remove(self._token_path)
                else:
                    logger.info("Token file changed externally, keeping it")
            except OSError:
                pass
        self._token_mtime = None

    def is_authenticated(self) -> bool:
        """Check whether the client has a valid OAuth session.

        Attempts to restore from a cached token if needed. Returns True/False
        without raising.
        """
        if self._oauth_client is not None:
            if self._token_changed_on_disk():
                logger.info("Token file updated externally, reloading")
                self._oauth_client = None
                self._token_mtime = None
            elif self._is_token_expired():
                logger.info("Active session token expired, invalidating")
                self._invalidate()
                return False
            else:
                return True

        if self._is_token_expired():
            logger.info("Cached token expired, removing")
            self._remove_token()

        token = self._load_cached_token()
        if token:
            logger.info("Restoring session from cached token")
            self._oauth_client = OAuth1Client(
                client_key=self._consumer_key,
                client_secret=self._consumer_secret,
                resource_owner_key=token["oauth_token"],
                resource_owner_secret=token["oauth_token_secret"],
                signature_type="AUTH_HEADER",
            )
            if self._renew_session():
                logger.info("Session renewed successfully")
                return True
            # _renew_session already cleared _oauth_client on failure
            return False

        logger.info("No cached token available")
        return False

    def get_authorization_url(self) -> str:
        """Start the OAuth flow and return the authorization URL.

        The caller should direct the user to this URL so they can authorize
        the application and obtain a verification code.
        """
        request_client = OAuth1Client(
            client_key=self._consumer_key,
            client_secret=self._consumer_secret,
            callback_uri="oob",
            signature_type="AUTH_HEADER",
        )
        url = f"{self._base_url}/oauth/request_token"
        signed_url, signed_headers, _ = request_client.sign(url, http_method="POST")
        response = self._http.post(signed_url, headers=signed_headers)
        response.raise_for_status()
        self._pending_request_token = dict(parse_qsl(response.text))
        logger.info("Obtained request token, awaiting user authorization")

        return (
            f"{AUTH_TOKEN_URL}"
            f"?key={self._consumer_key}"
            f"&token={self._pending_request_token['oauth_token']}"
        )

    def complete_authorization(self, verification_code: str) -> None:
        """Complete the OAuth flow with the user-provided verification code.

        Must be called after get_authorization_url(). Sets up the OAuth client
        and saves the access token.
        """
        if self._pending_request_token is None:
            raise RuntimeError(
                "No pending request token — call get_authorization_url() first"
            )

        access_client = OAuth1Client(
            client_key=self._consumer_key,
            client_secret=self._consumer_secret,
            resource_owner_key=self._pending_request_token["oauth_token"],
            resource_owner_secret=self._pending_request_token["oauth_token_secret"],
            verifier=verification_code,
            signature_type="AUTH_HEADER",
        )
        url = f"{self._base_url}/oauth/access_token"
        signed_url, signed_headers, _ = access_client.sign(url, http_method="POST")
        response = self._http.post(signed_url, headers=signed_headers)
        response.raise_for_status()

        token = dict(parse_qsl(response.text))
        logger.info("Authorization complete, saving access token")
        self._save_token(token)
        self._oauth_client = OAuth1Client(
            client_key=self._consumer_key,
            client_secret=self._consumer_secret,
            resource_owner_key=token["oauth_token"],
            resource_owner_secret=token["oauth_token_secret"],
            signature_type="AUTH_HEADER",
        )
        self._pending_request_token = None

    # -- API methods --

    def fetch_accounts(self) -> list[dict]:
        """Fetch and return the list of accounts."""
        url = f"{self._base_url}/v1/accounts/list"
        headers = {"Accept": "application/json"}

        response = self._get(url, headers=headers)
        response.raise_for_status()

        accounts_data = response.json()["AccountListResponse"]["Accounts"]["Account"]
        return accounts_data

    def fetch_portfolio(
        self,
        account_id_key: str,
        view: str | None,
    ) -> list[dict[str, Any]]:
        """Fetch and return a list of positions for the given key"""
        url = f"{self._base_url}/v1/accounts/{account_id_key}/portfolio"
        headers = {"Accept": "application/json"}
        params = {"view": view}
        response = self._get(url, headers=headers, params=params)
        response.raise_for_status()
        portfolio_response = response.json()["PortfolioResponse"]
        if len(portfolio_response["AccountPortfolio"]) != 1:
            raise ValueError(
                f"Unexpected accountPortfolio length "
                f"{len(portfolio_response['accountPortfolio'])} "
                f"in {pformat(portfolio_response)}"
            )

        account_portfolio = portfolio_response["AccountPortfolio"][0]
        if account_portfolio.get("next"):
            raise Exception("Paging not yet supported")

        return account_portfolio["Position"]

    def fetch_quotes_for(
        self, symbols: list[str], detail_flag: str | None = None
    ) -> list[dict[str, Any]]:
        _MAX_SYMBOLS_PER_REQUEST = 25
        symbol_chunks = [
            symbols[i : i + _MAX_SYMBOLS_PER_REQUEST]
            for i in range(0, len(symbols), _MAX_SYMBOLS_PER_REQUEST)
        ]

        result = []
        for chunk in symbol_chunks:
            symbol_query_str = ",".join(chunk)
            url = f"{self._base_url}/v1/market/quote/{symbol_query_str}"
            headers = {"Accept": "application/json"}
            params = {"detailFlag": detail_flag}
            response = self._get(url, headers=headers, params=params)
            response.raise_for_status()

            result.extend(response.json()["QuoteResponse"]["QuoteData"])

        return result

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

        url = f"{self._base_url}/v1/accounts/{account_id_key}/orders"
        headers = {"Accept": "application/json"}
        params = {"fromDate": start_date, "toDate": end_date, "status": status}
        response = self._get(url, headers=headers, params=params)
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
            response = self._get(url, headers=headers, params=params)
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

        response = self._get(details_url, headers=headers)
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

        url = f"{self._base_url}/v1/accounts/{account_id_key}/transactions"
        headers = {"Accept": "application/json"}
        params = {"fromDate": start_date, "toDate": end_date}
        response = self._get(url, headers=headers, params=params)
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
            response = self._get(transaction_list_response.get("next"), headers=headers)
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
            f"{self._base_url}/v1/accounts/{account_id_key}/"
            f"transactions/{transactionId}"
        )
        response = self._get(url, headers=headers)
        response.raise_for_status()

        return response.json().get("TransactionDetailsResponse")
