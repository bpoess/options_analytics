import webbrowser

from etrade_client.cache_client import ETradeCachedClient
from etrade_client.client import ETradeClient


def ensure_authenticated(client: ETradeClient | ETradeCachedClient) -> None:
    """Ensure the client has a valid OAuth session, prompting if needed."""
    if client.is_authenticated():
        return
    auth_url = client.get_authorization_url()
    webbrowser.open(auth_url)
    code = input("Please accept agreement and enter text code from browser: ")
    client.complete_authorization(code)
