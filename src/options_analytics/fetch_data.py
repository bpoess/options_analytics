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

from options_analytics import config
from options_analytics.clients.etrade.client import ETradeClient

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
        root_logger.addHandler(fh)


configure_logging()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def main() -> int:
    logger.info(f"Date range: {args.startdate} to {args.enddate}")

    config.initialize()

    client = ETradeClient(
        config.etrade_consumer_key(), config.etrade_consumer_secret(), sandbox=False
    )

    json_data = {}
    json_data["Accounts"] = []
    json_data["OrderList"] = {}
    json_data["OrderDetails"] = {}
    json_data["Transactions"] = {}
    json_data["TransactionDetails"] = {}

    fetched_accounts = client.fetch_accounts()
    configured_accounts = config.account_ids()
    accounts = list(
        filter(lambda x: x.get("accountId") in configured_accounts, fetched_accounts)
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
            args.startdate,
            args.enddate,
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
            client.fetch_transactions(account_id_key, args.startdate, args.enddate)
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

    with open(args.out, "w") as json_file:
        json.dump(json_data, json_file, indent=4)
        logger.info(f"Stored data in {args.out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
