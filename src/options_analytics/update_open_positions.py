from multiprocessing import freeze_support

# Do this as early as possible to avoid executing the code below twice
# See https://pyinstaller.org/en/stable/common-issues-and-pitfalls.html#multi-processing
if __name__ == "__main__":
    freeze_support()

import argparse
import logging
import logging.handlers
import time
from datetime import datetime
from pprint import pformat

from options_analytics.clients.etrade import models as etrade_models
from options_analytics.clients.etrade.client import ETradeClient
from options_analytics.config import Config
from options_analytics.models import CallOrPut, OptionPosition, OptionQuote
from options_analytics.worksheet import Worksheet

# globals
logger: logging.Logger


class Account:
    id: str
    label: str
    id_key: str

    def __init__(self, id: str, label: str, id_key: str):
        self.id = id
        self.label = label
        self.id_key = id_key


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

    fh = logging.handlers.RotatingFileHandler(
        args.logfile, maxBytes=10 * 1024 * 1024, backupCount=10
    )
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    root_logger.addHandler(fh)

    global logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "google_sheet_id",
        help="Google Speadsheet ID of the Options Tracker sheet",
    )
    parser.add_argument(
        "--loglevel",
        default="WARNING",
        help="Log level (DEBUG, INFO, WARNING, ERROR) for stdout logging",
    )
    parser.add_argument(
        "--logfile",
        default="update_open_positions.log",
        help="Path to file target for logs. File logs will be at debug level.",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Continuously update the Open Positions tab",
    )
    parser.add_argument(
        "--update-frequency", default=30, help="Update frequency in seconds"
    )
    return parser.parse_args()


def list_positions(
    client: ETradeClient, accounts: list[Account]
) -> list[OptionPosition]:
    result = []
    for account in accounts:
        logger.debug(f"Gathering positions for {account.label}")
        portfolio_data_list = client.fetch_portfolio(account.id_key, None)

        for position_data in portfolio_data_list:
            logger.debug(f"Position data: {pformat(position_data)}")
            etrade_position = etrade_models.Position.model_validate(position_data)
            if etrade_position.product.security_type == "OPTN":
                position = OptionPosition()
                position.id = etrade_position.id
                position.account_id = account.id
                position.account_label = account.label
                position.date_acquired = etrade_position.date_acquired
                position.cost_basis = etrade_position.cost_per_share
                position.commission = etrade_position.commissions
                position.fees = etrade_position.other_fees
                position.symbol = etrade_position.product.symbol
                product = etrade_position.product
                if (
                    not product.call_put
                    or not product.expiry_year
                    or not product.expiry_month
                    or not product.expiry_day
                    or not product.strike_price
                ):
                    raise ValueError(f"Invalid option product {product}")

                position.call_or_put = CallOrPut.from_str(product.call_put)
                position.expiry_year = product.expiry_year
                position.expiry_month = product.expiry_month
                position.expiry_day = product.expiry_day
                position.strike_price = product.strike_price

                result.append(position)

    return result


def update_quotes(client: ETradeClient, option_positions: list[OptionPosition]):
    quote_to_positions_index = {}

    for position in option_positions:
        positions_for_symbol = quote_to_positions_index.get(position.quote_key)
        logger.debug(f"Adding {position.id} {position.quote_key} into index")
        if positions_for_symbol:
            positions_for_symbol.append(position)
        else:
            quote_to_positions_index[position.quote_key] = [position]

    logger.debug("Gathering quotes")
    quotes_to_gather = list(quote_to_positions_index)
    quotes_data = client.fetch_quotes_for(quotes_to_gather, "OPTIONS")
    for quote_data in quotes_data:
        logger.debug(f"Quote data: {pformat(quote_data)}")
        etrade_quote = etrade_models.Quote.model_validate(quote_data)
        if not etrade_quote.option_details:
            raise ValueError(f"Expected quote to contain details {etrade_quote}")
        option_details = etrade_quote.option_details

        quote = OptionQuote()
        quote.date = etrade_quote.date_time.astimezone()
        quote.mark = option_details.bid
        quote.intrinsic = option_details.intrinsic_value
        quote.days_to_expiration = option_details.days_to_expiration

        positions = quote_to_positions_index[etrade_quote.product.quote_key]
        for position in positions:
            position.quote = quote


def main() -> int:
    args = parse_args()
    configure_logging(args)

    config = Config.from_file("config.toml")

    client = ETradeClient(config.etrade.key.api, config.etrade.key.secret)

    accounts = []
    etrade_accounts = client.fetch_accounts()
    for etrade_account in etrade_accounts:
        configured_account = config.etrade.find_account_by_id(
            etrade_account["accountId"]
        )
        if not configured_account:
            continue

        account = Account(
            etrade_account["accountId"],
            configured_account.label,
            etrade_account["accountIdKey"],
        )
        accounts.append(account)

    if args.continuous:
        print("Updating continuously, CTRL+C to interrupt.")
    try:
        while True:
            print(
                f"{datetime.now().isoformat(timespec='seconds')} Updating tab...",
                end="",
                flush=True,
            )
            option_positions = list_positions(client, accounts)
            update_quotes(client, option_positions)

            worksheet = Worksheet(args.google_sheet_id)
            tab = worksheet.open_positions_tab()
            tab.update_tab(option_positions)

            if args.continuous:
                print("Done, next update in 30s", flush=True)
                time.sleep(args.update_frequency)
            else:
                print("Done", flush=True)
                break
    except KeyboardInterrupt:
        pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
