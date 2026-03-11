from __future__ import annotations

from multiprocessing import freeze_support

# Do this as early as possible to avoid executing the code below twice
# See https://pyinstaller.org/en/stable/common-issues-and-pitfalls.html#multi-processing
if __name__ == "__main__":
    freeze_support()

import argparse
import json
import logging
from datetime import datetime, timedelta

from tqdm import tqdm

import options_analytics.etrade as etrade
from options_analytics.config import Config
from options_analytics.models import (
    OptionTransaction,
    TransactionCategory,
    TransactionKind,
)
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


class TransactionRepository:
    transactions: list[OptionTransaction]
    # Maps account ID + order ID -> [OptionTransaction]
    _order_index: dict[str, list[OptionTransaction]]

    def __init__(self, transactions: list[OptionTransaction]):
        logger.debug(f"Storing {len(transactions)} transactions")
        self.transactions = transactions

        self._order_index = {}
        for transaction in transactions:
            if transaction.order_id is not None:
                key = transaction.account_id + ":" + transaction.order_id
                if self._order_index.get(key) is None:
                    self._order_index[key] = []

                self._order_index[key].append(transaction)

    def find_transactions_for_order_id(
        self, account_id: str, order_id: str
    ) -> list[OptionTransaction]:
        return self._order_index.get(f"{account_id}:{order_id}", [])


class OptionTransactionsProcessor:
    _start_date: str
    _end_date: str
    _config: Config
    _worksheet: Worksheet
    _etrade_repository: etrade.Repository
    _transaction_repository: TransactionRepository

    def __init__(
        self, args: argparse.Namespace, config: Config, json_data: dict | None = None
    ):
        self._start_date = args.startdate
        self._end_date = args.enddate
        self._config = config
        logger.debug(f"startdate {self._start_date}, enddate {self._end_date}")
        self._etrade_repository = etrade.Repository(config.etrade, json_data)

        self._worksheet = Worksheet(args.google_sheet_id)

    def fetch_data(self):
        transactions = [
            transaction
            for transaction in self._etrade_repository.list_option_transactions(
                self._start_date, self._end_date
            )
            if not self._worksheet.has_transaction_been_processed(transaction.id)
        ]
        self._transaction_repository = TransactionRepository(transactions)

    def _is_part_of_roll_order(self, account_id: str, order_id: str) -> bool:
        transactions = self._transaction_repository.find_transactions_for_order_id(
            account_id, order_id
        )
        assert len(transactions) != 0

        sell_open = 0
        buy_close = 0
        for transaction in transactions:
            match transaction.kind:
                case TransactionKind.SELL_OPEN:
                    sell_open += 1
                case TransactionKind.BUY_CLOSE:
                    buy_close += 1
                case _:
                    raise ValueError(
                        f"Order {order_id} Unexpected kind value {transaction}"
                    )

        return sell_open > 0 and buy_close > 0

    def _classify_transaction(self, transaction: OptionTransaction):
        if transaction.order_id is not None:
            transaction.is_part_of_roll_order = self._is_part_of_roll_order(
                transaction.account_id, transaction.order_id
            )

        match transaction.kind:
            case TransactionKind.SELL_OPEN:
                transaction.category = TransactionCategory.OPEN
            case TransactionKind.BUY_CLOSE:
                # Bought to close
                if transaction.is_part_of_roll_order:
                    transaction.category = TransactionCategory.ROLL
                else:
                    transaction.category = TransactionCategory.CLOSED_EARLY
            case TransactionKind.EXPIRED:
                transaction.category = TransactionCategory.EXPIRED
            case TransactionKind.ASSIGNED:
                transaction.category = TransactionCategory.ASSIGNED
            case _:
                raise ValueError(f"Unexpected kind for {transaction}")

    def classify_transactions(self):
        for transaction in self._transaction_repository.transactions:
            self._classify_transaction(transaction)
            logger.debug(
                f"TC {transaction.category} R {transaction.is_part_of_roll_order} "
                f"{transaction}"
            )

    def generate_worksheet_updates(self):
        if len(self._transaction_repository.transactions) == 0:
            print("No new transactions to process")
            return

        manual_interventions = []
        with tqdm(
            total=len(self._transaction_repository.transactions),
            desc="Processing transactions",
            leave=True,
        ) as progress_bar:
            for transaction in self._transaction_repository.transactions:
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

        if len(manual_interventions) > 0:
            logger.debug("Manual Intervention Needed:")
            print("Unable to process the following entries:")
            for transaction in manual_interventions:
                print(transaction.format_for_script_output())
                logger.debug(transaction)

    def upload_worksheet_changes(self):
        self._worksheet.upload_changes()


def lookup_user_data(json_data: dict, username: str) -> dict | None:
    for user in json_data["users"]:
        if user["username"] == username:
            return user


def main() -> int:
    json_data = None
    if args.fromcache:
        with open(args.fromcache) as json_file:
            json_data = json.load(json_file)

        if json_data["version"] != 2:
            raise Exception(f"Unsupported cache data version {json_data['version']}")

    processor = OptionTransactionsProcessor(args, config, json_data)

    processor.fetch_data()
    processor.classify_transactions()
    processor.generate_worksheet_updates()
    processor.upload_worksheet_changes()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
