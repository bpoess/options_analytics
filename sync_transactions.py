import argparse
import logging
from datetime import datetime, timedelta

import config
import db
from clients.etrade import ETradeClient

logger = logging.getLogger(__name__)

current_date = datetime.now()
seven_days_ago = current_date - timedelta(days=7)
formatted_seven_days_ago = seven_days_ago.strftime("%m%d%Y")
formatted_current_date = current_date.strftime("%m%d%Y")

parser = argparse.ArgumentParser(
    description="Fetch E*Trade transactions and store in database."
)
parser.add_argument(
    "-s", "--startdate", default=formatted_seven_days_ago, help="Start Date (MMDDYYYY)"
)
parser.add_argument(
    "-e", "--enddate", default=formatted_current_date, help="End Date (MMDDYYYY)"
)
parser.add_argument(
    "--sandbox", action="store_true", help="Use sandbox backend instead of production"
)
parser.add_argument(
    "--log", default="WARNING", help="Log level (DEBUG, INFO, WARNING, ERROR)"
)
args = parser.parse_args()


def sync_accounts(accounts, account_repo):
    """Sync fetched accounts with database."""
    for account in accounts:
        existing = account_repo.get_by_id(account.account_id)
        if existing is None:
            account_repo.create(account)
            logger.info(f"Created account {account.account_id}")
        elif existing != account:
            account_repo.update(account)
            logger.info(f"Updated account {account.account_id}")
        else:
            logger.debug(f"Account {account.account_id} unchanged")


def sync_transactions(transactions, transaction_repo):
    """Sync fetched transactions with database."""
    for tx in transactions:
        existing = transaction_repo.get_by_id(tx.transaction_id)
        if existing is None:
            transaction_repo.create(tx)
            logger.info(f"Created transaction {tx.transaction_id}")
        elif existing != tx:
            transaction_repo.update(tx)
            logger.info(f"Updated transaction {tx.transaction_id}")
        else:
            logger.debug(f"Transaction {tx.transaction_id} unchanged")


if __name__ == "__main__":
    logging.basicConfig(level=getattr(logging, args.log.upper()))

    config.initialize()
    db.create_if_not_exist()

    client = ETradeClient(sandbox=args.sandbox)
    account_repo = db.repositories.AccountRepository()
    transaction_repo = db.repositories.TransactionRepository()

    # Fetch and sync accounts
    accounts = client.get_accounts()
    sync_accounts(accounts, account_repo)
    logger.info(f"Synced {len(accounts)} accounts")

    # Fetch and sync transactions for each account
    total_transactions = 0
    for account in accounts:
        transactions = client.get_transactions(
            account.account_id_key, args.startdate, args.enddate
        )
        sync_transactions(transactions, transaction_repo)
        total_transactions += len(transactions)
        logger.info(
            f"Synced {len(transactions)} transactions for account {account.account_id}"
        )

    logger.info(f"Synced {total_transactions} transactions total")
