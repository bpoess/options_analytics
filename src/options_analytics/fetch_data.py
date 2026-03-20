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

from etrade_client.client import ETradeClient
from options_analytics.auth import ensure_authenticated
from options_analytics.config import Config

# Constants
current_date = datetime.now()
seven_days_ago = current_date - timedelta(days=7)
# Orders API uses MMDDYYYY format (no slashes)
formatted_seven_days_ago = seven_days_ago.strftime("%m%d%Y")
formatted_current_date = current_date.strftime("%m%d%Y")


parser = argparse.ArgumentParser(
    description="Fetch E*Trade orders and store in given outfile in JSON format."
)
parser.add_argument(
    "out",
    help="Path to file to store response data in JSON format",
)
parser.add_argument(
    "-s",
    "--startdate",
    default=formatted_seven_days_ago,
    help="Start Date (MMDDYYYY)",
)
parser.add_argument(
    "-e", "--enddate", default=formatted_current_date, help="End Date (MMDDYYYY)"
)
parser.add_argument(
    "--loglevel",
    default="INFO",
    help="Log level (DEBUG, INFO, WARNING, ERROR) for stdout logging",
)
parser.add_argument(
    "--logfile",
    default=None,
    help="Path to file target for logs. File logs will be at debug level.",
)


def configure_logging(args):
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
        root_logger.addHandler(fh)


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def fetch_data(
    config: Config,
    json_data: dict,
    start_date: str,
    end_date: str,
):
    client = ETradeClient(
        config.etrade.key.api, config.etrade.key.secret, sandbox=False
    )
    ensure_authenticated(client)

    json_data["Accounts"] = []
    json_data["OrderList"] = {}
    json_data["OrderDetails"] = {}
    json_data["Transactions"] = {}
    json_data["TransactionDetails"] = {}

    fetched_accounts = client.fetch_accounts()
    accounts = list(
        filter(
            lambda x: config.etrade.find_account_by_id(x["accountId"]) is not None,
            fetched_accounts,
        )
    )
    json_data["Accounts"] = accounts

    logger.info(f"Fetching data for {len(accounts)} accounts")

    total_orders = 0
    total_transactions = 0
    for account in accounts:
        accountId = account.get("accountId")
        account_id_key = account["accountIdKey"]

        logger.info(f"Fetching order list for account {accountId}...")
        order_list = json_data["OrderList"][accountId] = client.fetch_order_list(
            account_id_key,
            start_date,
            end_date,
        )

        order_details = json_data["OrderDetails"][accountId] = {}
        with tqdm(
            total=len(order_list), desc="Fetching order details", leave=True
        ) as progress_bar:
            for order in order_list:
                order_details[order["orderId"]] = client.fetch_order_details(order)
                progress_bar.update(1)
        total_orders += len(order_list)

        logger.info(f"Fetching transactions for account {accountId}...")
        transaction_list = json_data["Transactions"][accountId] = (
            client.fetch_transactions(account_id_key, start_date, end_date)
        )
        transaction_details = json_data["TransactionDetails"][accountId] = {}
        with tqdm(
            total=len(transaction_list), desc="Fetching transaction details", leave=True
        ) as progress_bar:
            for transaction in transaction_list:
                transactionId = str(transaction["transactionId"])
                transaction_details[transactionId] = client.fetch_transaction_details(
                    account_id_key, transactionId
                )
                progress_bar.update(1)

        total_transactions += len(transaction_list)

    logger.info(f"Fetched {total_orders} orders and {total_transactions} transactions")


def main() -> int:
    config = Config.from_file("config.toml")
    args = parser.parse_args()

    configure_logging(args)

    logger.info(f"Date range: {args.startdate} to {args.enddate}")

    json_data = {"version": 2}

    fetch_data(config, json_data, args.startdate, args.enddate)

    with open(args.out, "w") as json_file:
        json.dump(json_data, json_file, indent=4)
        logger.info(f"Stored data in {args.out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
