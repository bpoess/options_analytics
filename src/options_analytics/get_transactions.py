from multiprocessing import freeze_support

# Do this as early as possible to avoid executing the code below twice
# See https://pyinstaller.org/en/stable/common-issues-and-pitfalls.html#multi-processing
if __name__ == "__main__":
    freeze_support()

import argparse
import json
import logging
import logging.handlers
from datetime import datetime, timedelta
from typing import NamedTuple

from etrade_client.cache_client import ETradeCachedClient
from etrade_client.models import Account, Transaction
from options_analytics.auth import ensure_authenticated
from options_analytics.config import Config

current_date = datetime.now()
seven_days_ago = current_date - timedelta(days=7)
formated_seven_days_ago = seven_days_ago.strftime("%m%d%Y")
formatted_current_date = current_date.strftime("%m%d%Y")


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
    "-s", "--startdate", default=formated_seven_days_ago, help="Start Date"
)
parser.add_argument("-e", "--enddate", default=formatted_current_date, help="End Date")
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
    default="get_transactions.log",
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

    fh = logging.handlers.RotatingFileHandler(
        args.logfile, maxBytes=10 * 1024 * 1024, backupCount=10
    )
    fh.setFormatter(formatter)
    root_logger.addHandler(fh)


config = Config.from_file("config.toml")
configure_logging()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Contract(NamedTuple):
    symbol: str
    transaction_date: str
    expiry_date: str
    call_put: str
    bought_sold: str
    strike_price: str
    price: str
    quantity: str
    fee: str
    account_name: str

    @property
    def lot_id(self):
        return (
            self.symbol
            + self.expiry_date
            + self.call_put
            + self.strike_price
            + self.account_name
        )


def get_accounts(client: ETradeCachedClient, config: Config):
    """Returns an array of etrade Account dictionaries"""

    accounts = client.fetch_accounts()

    return list(
        filter(
            lambda account: config.etrade.find_account_by_id(account.id) is not None,
            map(lambda data: Account.model_validate(data), accounts),
        )
    )


def process_transaction(transactions, data_dict, error_list):
    for transaction in transactions:
        transaction_type_to_skip = [
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
        ]
        if transaction.brokerage.transaction_type in transaction_type_to_skip:
            logger.debug(f"Skipping {transaction}")
            continue

        if transaction.brokerage.product.call_put is not None:
            data_dict[transaction.id] = transaction
            logger.debug(f"Processing {transaction}")
        else:
            logger.debug(f"Can't process {transaction}")
            error_list.append(transaction)


def get_transactions(
    client: ETradeCachedClient,
    account: Account,
    data_dict: dict,
    error_list: list[Transaction],
):
    transactions = client.fetch_transactions(
        account.id_key, args.startdate, args.enddate
    )
    detailed_transactions = []
    for transaction in transactions:
        detailed_transactions.append(
            Transaction.model_validate(
                client.fetch_transaction_details(
                    account.id_key, transaction["transactionId"]
                )
            )
        )
    logger.debug(f"Found {len(detailed_transactions)} transactions")
    process_transaction(detailed_transactions, data_dict, error_list)


def format_transactions(
    config: Config,
    account: Account,
    data_dict: dict,
    contract_open_list: list[Contract],
    contract_closed_list: list,
    option_expire_list: list,
    assigned_list: list,
):
    # print out transactions in google sheet format.
    for key in sorted(data_dict):
        transaction = data_dict[key]
        product = transaction.brokerage.product

        symbol = product.symbol
        transaction_date = transaction.date.strftime("%m/%d/%y")
        expiry_month = f"{product.expiry_month:02}"
        expiry_day = f"{product.expiry_day:02}"
        expiry_date = expiry_month + "/" + expiry_day + "/" + str(product.expiry_year)
        call_put = product.call_put.capitalize()
        strike_price = f"{product.strike_price:,f}"
        price = f"{transaction.brokerage.price:,f}"
        quantity = f"{abs(transaction.brokerage.quantity):,f}"
        fee = f"{transaction.brokerage.fee:,f}"

        account_name = next(
            v.label for v in config.etrade.accounts if v.id == account.id
        )

        action = transaction.brokerage.transaction_type
        if action == "Sold Short":
            bought_sold = "S"
        elif action == "Bought To Open":
            bought_sold = "B"
        else:
            bought_sold = " "

        if action in ["Sold Short", "Bought To Open"]:
            contract_open_list.append(
                Contract(
                    symbol,
                    transaction_date,
                    expiry_date,
                    call_put,
                    bought_sold,
                    strike_price,
                    price,
                    quantity,
                    fee,
                    account_name,
                )
            )

        if action in ["Bought To Cover", "Sold To Close"]:
            contract_closed_list.append(
                Contract(
                    symbol,
                    transaction_date,
                    expiry_date,
                    call_put,
                    bought_sold,
                    strike_price,
                    price,
                    quantity,
                    fee,
                    account_name,
                )
            )

        if action in ["Option Expired"]:
            option_expire_list.append(
                Contract(
                    symbol,
                    transaction_date,
                    expiry_date,
                    call_put,
                    bought_sold,
                    strike_price,
                    price,
                    quantity,
                    fee,
                    account_name,
                )
            )
        if action in ["Option Assigned"]:
            assigned_list.append(
                Contract(
                    symbol,
                    transaction_date,
                    expiry_date,
                    call_put,
                    bought_sold,
                    strike_price,
                    price,
                    quantity,
                    fee,
                    account_name,
                )
            )
        """
                    transaction_date,
                    symbol,
                    expiry_date,
                    call_put,
                    strike_price,
                    account_name,
                    quantity,
        """


def query_account(
    config: Config,
    client: ETradeCachedClient,
    account: Account,
):
    data_dict = {}
    error_list = []
    contract_open_list = []
    contract_closed_list = []
    option_expire_list = []
    assigned_list = []

    get_transactions(client, account, data_dict, error_list)

    format_transactions(
        config,
        account,
        data_dict,
        contract_open_list,
        contract_closed_list,
        option_expire_list,
        assigned_list,
    )

    print(f"Account {account.id}")

    print("New Positions")
    for item in contract_open_list:
        print(
            item.symbol,
            "\t",
            item.transaction_date,
            "\t",
            item.expiry_date,
            "\t",
            item.call_put,
            "\t",
            item.bought_sold,
            "\t",
            item.strike_price,
            "\t",
            item.price,
            "\t",
            item.quantity,
            "\t",
            item.fee,
            "\t",
            item.account_name,
        )

    print("\nClosed Positions")
    for contract in contract_closed_list:
        search_record = contract.lot_id
        print(
            search_record.ljust(35),
            "\t",
            contract.quantity.ljust(3),
            "\t"
            + contract.fee
            + "\t"
            + contract.price
            + "\t"
            + contract.transaction_date
            + "\tClosed",
        )

    print("\nExpired Positions")
    for contract in option_expire_list:
        print(
            contract.lot_id.ljust(35),
            "\t",
            contract.quantity.ljust(3),
            "\t0\t0\t" + contract.expiry_date + "\tClosed\tExpired",
        )

    print("\nAssigned Positions")
    for contract in assigned_list:
        print(
            contract.lot_id.ljust(35),
            "\t",
            contract.quantity.ljust(3),
            "\t0\t0\t" + contract.transaction_date + "\tClosed\tAssigned",
        )

    print("\nUnmatched Records")
    for item in error_list:
        print(item)


def main() -> int:

    json_data = None
    if args.fromcache:
        with open(args.fromcache) as json_file:
            json_data = json.load(json_file)

        if json_data["version"] != 2:
            raise Exception(f"Unsupported cache data version {json_data['version']}")

    client = ETradeCachedClient(
        config.etrade.key.api, config.etrade.key.secret, json_data
    )
    ensure_authenticated(client)

    accounts = get_accounts(client, config)
    for account in accounts:
        query_account(config, client, account)
        print("\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
