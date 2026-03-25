class AuthenticationRequired(Exception):
    """Raised when the client needs interactive authorization."""


class Timeout(Exception):
    """A request has timed out."""


class Internal(Exception):
    """An unspecified error occured."""
