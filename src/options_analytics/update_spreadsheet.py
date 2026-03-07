from __future__ import annotations

from multiprocessing import freeze_support

# Do this as early as possible to avoid executing the code below twice
# See https://pyinstaller.org/en/stable/common-issues-and-pitfalls.html#multi-processing
if __name__ == "__main__":
    freeze_support()

import argparse
import logging
from datetime import datetime, timedelta

from tqdm import tqdm

from options_analytics.clients.etrade.cache_client import ETradeCachedClient
from options_analytics.config import Config, UserConfig
from options_analytics.models import Account, TransactionCategory
from options_analytics.worksheet import Worksheet

CURRENT_DATE = datetime.now()
SEVEN_DAYS_AGO = CURRENT_DATE - timedelta(days=7)
FORMATED_SEVEN_DAYS_AGO = SEVEN_DAYS_AGO.strftime("%m%d%Y")
FORMATED_CURRENT_DATE = CURRENT_DATE.strftime("%m%d%Y")


# argparse
def csv_to_strings(s: str) -> list[str]:
    # "a,b, c" -> ["a", "b", "c"]
    # also ignores empty items like "a,,b"
    return [item.strip() for item in s.split(",") if item.strip()]


# Parse Arguments
parser = argparse.ArgumentParser(
    description="Example:  %(prog)s -s 09112023 -e 09222023"
)
parser.add_argument(
    "google_sheet_id",
    help="Google Speadsheet ID of the Options Tracker sheet",
)
parser.add_argument(
    "-s", "--startdate", default=FORMATED_SEVEN_DAYS_AGO, help="Start Date"
)
parser.add_argument("-e", "--enddate", default=FORMATED_CURRENT_DATE, help="End Date")
parser.add_argument(
    "--accounts",
    type=csv_to_strings,
    default=None,
    help="Comma-separated list of accounts to query, e.g. --names 365432,23214",
)
parser.add_argument(
    "--fromcache",
    default=None,
    help="Process data from cache instead of E*Trade backend",
)
parser.add_argument(
    "--loglevel",
    default="WARNING",
    help="Log level (DEBUG, INFO, WARNING, ERROR) for stdout logging",
)
parser.add_argument(
    "--logfile",
    default=None,
    help="Path to file target for logs. File logs will be at debug level.",
)
args = parser.parse_args()


def configure_logging():
    root_logger = logging.getLogger()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    # Configure a logger to console and one to a file if desired
    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, args.loglevel.upper()))
    ch.setFormatter(formatter)
    root_logger.addHandler(ch)
    if args.logfile:
        fh = logging.FileHandler(args.logfile)
        fh.setFormatter(formatter)
        fh.setLevel(logging.DEBUG)
        root_logger.addHandler(fh)


config = Config.from_file("config.toml")
configure_logging()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class ETradeProcessor:
    _client: ETradeCachedClient
    _start_date: str
    _end_date: str
    # Configured Accounts for processing [ID:Label]
    _user: UserConfig
    # Fetched ETrade Accounts
    _accounts: list[Account]
    _worksheet: Worksheet

    def _is_configured_account(self, id: str) -> bool:
        for account in self._user.etrade.accounts:
            if account.id == id:
                return True

        return False

    def _get_label_for_configured_account(self, id: str) -> str:
        for account in self._user.etrade.accounts:
            if account.id == id:
                return account.label

    def _fetch_accounts(self) -> list[Account]:
        with tqdm(total=0, desc="Fetching accounts", leave=True) as progress_bar:
            result = []
            accounts = self._client.fetch_accounts()

            for data in accounts:
                account = Account.model_validate(data)
                if not self._is_configured_account(account.id):
                    logger.debug(f"Not configured {account}, ignoring")
                    continue

                account._configured_name = self._get_label_for_configured_account(
                    account.id
                )
                result.append(account)
                progress_bar.total += 1
                progress_bar.update(1)

            return result

    def __init__(self, args: argparse.Namespace, user: UserConfig):
        self._start_date = args.startdate
        self._end_date = args.enddate
        self._user = user
        logger.debug(
            f"startdate {self._start_date}, enddate {self._end_date}, user {self._user}"
        )

        self._worksheet = Worksheet(args.google_sheet_id)

        self._client = ETradeCachedClient(
            self._user.etrade.key.api,
            self._user.etrade.key.secret,
            args.fromcache,
        )

        logger.debug("Ensuring connectivity")
        self._client.fetch_accounts()

    def fetch_data(self):
        self._accounts = self._fetch_accounts()
        logger.debug(f"Fetched {len(self._accounts)} accounts")

        for account in self._accounts:
            account.fetch_orders(self._client, self._start_date, self._end_date)
            account.fetch_transactions(self._client, self._start_date, self._end_date)

    def process_transactions(self):
        transactions = []
        for account in self._accounts:
            for transaction in account.transactions:
                if self._worksheet.has_transaction_been_processed(transaction.id):
                    logger.debug(f"{transaction} already processed")
                    continue
                account.classify_transaction(transaction)
                transactions.append(transaction)

        if len(transactions) == 0:
            print("Nothing to do, have a great day!")
            return

        transactions.sort(key=lambda t: (t.date, t.id))

        manual_interventions = []
        with tqdm(
            total=len(transactions),
            desc="Processing transactions",
            leave=True,
        ) as progress_bar:
            for transaction in transactions:
                category = transaction.category
                logger.debug(f"C {category} T {transaction}")
                if category == TransactionCategory.OPEN:
                    self._worksheet.add(transaction)
                elif (
                    category == TransactionCategory.CLOSED_EARLY
                    or category == TransactionCategory.ROLL
                    or category == TransactionCategory.ASSIGNED
                    or category == TransactionCategory.EXPIRED
                ):
                    success = self._worksheet.update(transaction, category)
                    if not success:
                        logger.debug(
                            f"Unable to update sheet for {transaction} {category}"
                        )
                        manual_interventions.append(transaction)
                else:
                    manual_interventions.append(transaction)

                progress_bar.update(1)

        self._worksheet.upload_changes()

        if len(manual_interventions) > 0:
            logger.debug("Manual Intervention Needed:")
            print("Unable to process the following entries:")
            for transaction in manual_interventions:
                print(transaction.format_for_script_output())
                logger.debug(transaction)


def main() -> int:

    for user in config.users:
        print(f"Processing transactions for {user.name}")
        processor = ETradeProcessor(args, user)

        processor.fetch_data()
        processor.process_transactions()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
