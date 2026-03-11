import json
import os
import sys
import webbrowser
from datetime import datetime
from pathlib import Path

from pytz import timezone
from requests_oauthlib import OAuth1Session

PROD_BASE_URL = "https://api.etrade.com"
SANDBOX_BASE_URL = "https://apisb.etrade.com"
AUTH_TOKEN_URL = "https://us.etrade.com/e/t/etws/authorize"


class ETradeSession:
    """Manages OAuth session lifecycle for E*Trade API."""

    def __init__(
        self,
        consumer_key: str,
        consumer_secret: str,
        token_path: str | Path = "access_token.json",
        sandbox: bool = False,
    ):
        self._consumer_key = consumer_key
        self._consumer_secret = consumer_secret
        self.token_path = Path(token_path)
        self.sandbox = sandbox
        self._session: OAuth1Session | None = None

    @property
    def base_url(self) -> str:
        """Return the API base URL for the current environment."""
        return SANDBOX_BASE_URL if self.sandbox else PROD_BASE_URL

    def get_session(self) -> OAuth1Session:
        """Get a valid OAuth session, authenticating if necessary."""
        if self._session is not None:
            return self._session

        # Remove expired token
        if self._is_token_expired():
            self._remove_token()

        # Try to load cached token
        token = self._load_cached_token()
        if token:
            self._session = self._create_session_from_token(token)
            self._renew_session()
        else:
            token = self._authorize()
            self._save_token(token)
            self._session = self._create_session_from_token(token)

        return self._session

    def _create_session_from_token(self, token: dict) -> OAuth1Session:
        """Create an OAuth1Session from a token dict."""
        return OAuth1Session(
            client_key=self._consumer_key,
            client_secret=self._consumer_secret,
            resource_owner_key=token["oauth_token"],
            resource_owner_secret=token["oauth_token_secret"],
            signature_type="AUTH_HEADER",
        )

    def _load_cached_token(self) -> dict | None:
        """Load token from disk if it exists."""
        if not self.token_path.exists():
            return None
        with open(self.token_path) as f:
            return json.load(f)

    def _save_token(self, token: dict):
        """Save token to disk with restricted permissions."""
        with open(self.token_path, "w") as f:
            json.dump(token, f)
        os.chmod(self.token_path, 0o600)

    def _remove_token(self):
        """Remove cached token file if it exists."""
        if self.token_path.exists():
            os.remove(self.token_path)

    def _is_token_expired(self) -> bool:
        """Check if token has expired (expires at midnight US Eastern)."""
        if not self.token_path.exists():
            return False
        tz = timezone("US/Eastern")
        mod_time = datetime.fromtimestamp(self.token_path.stat().st_mtime, tz)
        today_time = datetime.now(tz)
        return today_time.date() != mod_time.date()

    def _renew_session(self):
        """Renew the session token."""
        try:
            self._session.get(f"{self.base_url}/oauth/renew_access_token")
        except Exception:
            print("Unable to renew session, removing cached token.")
            print("Please try running command again.")
            self._remove_token()
            sys.exit(1)

    def _authorize(self) -> dict:
        """Run the OAuth authorization flow."""
        try:
            session = OAuth1Session(
                client_key=self._consumer_key,
                client_secret=self._consumer_secret,
                callback_uri="oob",
                signature_type="AUTH_HEADER",
            )
            session.fetch_request_token(f"{self.base_url}/oauth/request_token")
            authorization_url = session.authorization_url(AUTH_TOKEN_URL)
            akey = session.parse_authorization_response(authorization_url)
            resource_owner_key = akey["oauth_token"]

            formatted_auth_url = (
                f"{AUTH_TOKEN_URL}?key={self._consumer_key}&token={resource_owner_key}"
            )
            webbrowser.open(formatted_auth_url)
            text_code = input(
                "Please accept agreement and enter text code from browser: "
            )
            session._client.client.verifier = text_code

            return session.fetch_access_token(f"{self.base_url}/oauth/access_token")
        except Exception:
            print(
                "Unable to get authenticated session, "
                "check config for correct consumer_key and consumer_secret"
            )
            sys.exit(1)
