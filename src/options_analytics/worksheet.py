from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

import pygsheets
from pygsheets.client import Client as PygsheetsClient
from pygsheets.spreadsheet import Spreadsheet as PygsheetsSpreadsheet
from pygsheets.worksheet import Worksheet as PygsheetsWorksheet
from tqdm import tqdm

from options_analytics.models import (
    OptionPosition,
    OptionTransaction,
    TransactionCategory,
    TransactionKind,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

type TableRow = list[str]


class TrackerTabDataRow:
    # 0 Column A
    product_key: str = ""
    # 1 Column B
    symbol: str = ""
    # 2 Column C
    open_date: str = ""
    # 3 Column D
    expiry_date: str = ""
    # 4 Column E
    call_put: str = ""
    # 5 Column F
    # "B" or "S"
    bought_sold: str = ""
    # 6 Column G
    strike_price: str = ""
    # 7 Column H
    premium: str = ""
    # 8 Column I
    num_contracts: str = ""
    # 9 Column J
    open_fees: str = ""
    # 10 Column K
    account: str = ""
    # 11 Column L
    _DTE: str = "=if(isblank(B{row}),,max(D{row}-today(),0))"
    # 12 Column M
    _CURRENT_STOCK_PRICE: str = "=if(isblank(B{row}),,GoogleFinance(B{row}))"
    # 13 Column N
    _BREAK_EVEN_PRICE: str = (
        '=if(isblank(B{row}),,if(E{row}="Put",G{row}-H{row},G{row}+H{row}))'
    )
    # 14 Column O
    _PUT_CASH_RESERVE: str = (
        "=if(isblank(B{row}),,"
        'if(AND(E{row}="Put",F{row}="S"),G{row}*100*I{row},"---------"))'
    )
    # 15 Column P
    _PUT_MARGIN_CASH_RESERVE: str = ""  # Broken formula
    # 16 Column Q
    call_cost_basis_per_share: str = ""
    # 17 Column R
    _FEES: str = "=J{row}+S{row}"
    # 18 Column S
    closing_fees: str = ""
    # 19 Column T
    exit_price: str = ""
    # 20 Column U
    close_date: str = ""
    # 21 Column V
    status: str = ""
    # 22 Column W
    close_reason: str = ""
    # 23 Column X
    _PROFIT_LOSS: str = (
        "=if(isblank(B{row}),,"
        'if(F{row}="S",100*I{row}*(H{row}-T{row})-R{row},'
        "100*I{row}*(T{row}-H{row})-R{row}))"
    )
    # 24 Column Y
    _DAYS_HELD: str = (
        '=if(isblank(B{row}),,datedif(C{row},(if(isblank(U{row}),D{row},U{row})),"D"))'
    )
    # 25 Column Z
    _ANNUALIZED_ROR: str = (
        "=if(isblank(B{row}),,"
        'if(AND(E{row}="Put",F{row}="S"),'
        '(X{row}/O{row})/Y{row}*365,if(AND(E{row}="Call",F{row}="S"),'
        "(X{row}/(100*I{row}*G{row}))/Y{row}*365, "
        "(X{row}/(H{row}*I{row}*100)/Y{row}*365))))"
    )
    # 26 Column AA
    roll_id: str = ""
    # 27 Column AB
    transactions: str = ""

    def __init__(
        self,
        product_key: str,
        symbol: str,
        open_date: str,
        expiry_date: str,
        call_put: str,
        bought_sold: str,
        strike_price: str,
        premium: str,
        num_contracts: str,
        open_fees: str,
        account: str,
        status: str,
        roll_id: str,
        transactions: str,
    ):
        self.product_key = product_key
        self.symbol = symbol
        self.open_date = open_date
        self.expiry_date = expiry_date
        self.call_put = call_put
        self.bought_sold = bought_sold
        self.strike_price = strike_price
        self.premium = premium
        self.num_contracts = num_contracts
        self.open_fees = open_fees
        self.account = account
        self.status = status
        self.roll_id = roll_id
        self.transactions = transactions

    @classmethod
    def from_transaction(cls, transaction: OptionTransaction) -> TrackerTabDataRow:

        bought_sold = ""
        match transaction.kind:
            case TransactionKind.SELL_OPEN:
                bought_sold = "S"
            case _:
                raise ValueError(f"Unexpected kind in {transaction}")

        roll_id = ""
        if transaction.is_part_of_roll_order:
            assert transaction.order_id
            roll_id = transaction.order_id.strip()

        return cls(
            transaction.key,
            transaction.symbol,
            transaction.date.strftime("%m/%d/%y"),
            transaction.expiry_date,
            transaction.call_or_put,
            bought_sold,
            f"{transaction.strike_price:,f}",
            f"{transaction.price:,f}",
            f"{transaction.quantity:,f}",
            f"{transaction.fee:,f}",
            transaction.account_label,
            "Open",
            roll_id,
            transaction.id,
        )

    @classmethod
    def new_open(cls, row: TrackerTabDataRow) -> TrackerTabDataRow:
        return cls(
            row.product_key,
            row.symbol,
            row.open_date,
            row.expiry_date,
            row.call_put,
            row.bought_sold,
            row.strike_price,
            row.premium,
            row.num_contracts,
            row.open_fees,
            row.account,
            row.status,
            row.roll_id,
            row.transactions,
        )

    @classmethod
    def from_tracker_data(cls, data: list[Any]) -> TrackerTabDataRow:
        row = cls(
            data[0],  # Product Key
            data[1],  # Stock Symbol
            data[2],  # Open Date
            data[3],  # Exp Date
            data[4],  # Call or Put
            data[5],  # Bought or Sold
            data[6],  # Strike Price
            data[7],  # Premium
            data[8],  # C
            data[9],  # Open Fees
            data[10],  # Account
            data[21],  # Status
            data[26],  # Roll IDs
            data[27],  # Transactions
        )
        row.call_cost_basis_per_share = data[16]
        row.closing_fees = data[18]
        row.exit_price = data[19]
        row.close_date = data[20]
        row.close_reason = data[22]

        return row

    def _format(self) -> str:
        return (
            f"TrackerTabDataRow({self.product_key}|{self.symbol}|{self.open_date}|"
            f"{self.expiry_date}|{self.call_put}|{self.bought_sold}|"
            f"{self.strike_price}|{self.premium}|{self.num_contracts}|"
            f"{self.open_fees}|{self.account}|{self.closing_fees}|{self.exit_price}|"
            f"{self.close_date}|{self.status}|{self.close_reason}|{self.roll_id}|"
            f"{self.transactions})"
        )

    def __str__(self) -> str:
        return self._format()

    def __repr__(self) -> str:
        return self._format()

    def materialize_table_row(self, row_num: int) -> TableRow:
        return [
            self.product_key,
            self.symbol,
            self.open_date,
            self.expiry_date,
            self.call_put,
            self.bought_sold,
            self.strike_price,
            self.premium,
            self.num_contracts,
            self.open_fees,
            self.account,
            self._DTE.format(row=row_num),
            self._CURRENT_STOCK_PRICE.format(row=row_num),
            self._BREAK_EVEN_PRICE.format(row=row_num),
            self._PUT_CASH_RESERVE.format(row=row_num),
            self._PUT_MARGIN_CASH_RESERVE.format(row=row_num),
            self.call_cost_basis_per_share,
            self._FEES.format(row=row_num),
            self.closing_fees,
            self.exit_price,
            self.close_date,
            self.status,
            self.close_reason,
            self._PROFIT_LOSS.format(row=row_num),
            self._DAYS_HELD.format(row=row_num),
            self._ANNUALIZED_ROR.format(row=row_num),
            self.roll_id,
            self.transactions,
        ]


class TrackerTab:
    rows: dict[int, TrackerTabDataRow]
    next_empty_row: int
    _last_remote_row_with_data: int
    _tracker: PygsheetsWorksheet
    _product_key_to_row_num_index: dict[str, list[int]]
    _transactions_processed_index: dict[str, bool]

    _TRACKER_TAB_LABEL: str = "Puts/Calls"

    def __init__(self, gsheet: PygsheetsSpreadsheet):
        logger.debug("Initializing tracker tab")

        self.rows = {}
        self._product_key_to_row_num_index = {}
        self._transactions_processed_index = {}

        self._tracker = gsheet.worksheet_by_title(TrackerTab._TRACKER_TAB_LABEL)

        self.next_empty_row = (
            len(self._tracker.get_col(2, include_tailing_empty=False)) + 1
        )  # col 2 = B
        self._last_remote_row_with_data = self.next_empty_row - 1
        self._build_product_key_index()
        self._build_transactions_processed_index()

        logger.debug(f"TrackerTab Init Complete next_row={self.next_empty_row}")

    def _build_product_key_index(self):
        header_rows = 1
        col_a = self._tracker.get_col(1, include_tailing_empty=False)

        start = header_rows  # 0-based slice point
        for row_num, key in enumerate(col_a[start:], start=header_rows + 1):
            if not key:
                continue

            row_list = self._product_key_to_row_num_index.get(key)
            if row_list is None:
                self._product_key_to_row_num_index[key] = [row_num]
            else:
                row_list.append(row_num)

    def _build_transactions_processed_index(self):
        header_rows = 1
        col_transactions = self._tracker.get_col(28, include_tailing_empty=False)

        start = header_rows  # 0-based slice point
        for _unused, data in enumerate(col_transactions[start:], start=header_rows + 1):
            if not data:
                continue
            transaction_ids = [
                transaction_id.strip()
                for transaction_id in data.split(",")
                if transaction_id.strip()
            ]

            for transaction_id in transaction_ids:
                self._transactions_processed_index[transaction_id] = True

    def has_transaction_been_processed(self, transaction_id: str) -> bool:
        return self._transactions_processed_index.get(transaction_id, False)

    def upload_changes(self):
        remote_updates = sorted(self.rows.items(), key=lambda item: item[0])  # row_num

        if len(remote_updates) == 0:
            logger.debug("No changes, nothing to upload")
            return

        with tqdm(
            total=len(remote_updates), desc="Updating tracker sheet", leave=True
        ) as progress_bar:
            BATCH_SIZE = 50
            batch_start: int | None = None
            batch_end: int | None = None
            batch: list[TableRow] = []
            for row_num, worksheet_row in remote_updates:
                if batch_start is None:
                    batch_start = row_num
                    batch_end = row_num
                    batch = []
                else:
                    assert batch_start is not None
                    assert batch_end is not None
                    if (
                        row_num != (batch_end + 1)
                        or (batch_end - batch_start) == BATCH_SIZE
                    ):
                        logger.debug(f"Flush row A{batch_start}:AB{batch_end}")
                        self._tracker.update_values(
                            f"A{batch_start}:AB{batch_end}", batch
                        )
                        progress_bar.update(batch_end - batch_start + 1)
                        # Reset batch
                        batch_start = row_num
                        batch_end = row_num
                        batch = []
                    else:
                        assert row_num == batch_end + 1
                        batch_end = row_num

                data = worksheet_row.materialize_table_row(row_num)
                batch.append(data)

                logger.debug(f"[{row_num}] {worksheet_row}")

            # Flush leftovers from the batch
            if batch_start is not None:
                assert batch_end is not None
                logger.debug(f"Flush row A{batch_start}:AB{batch_end}")
                self._tracker.update_values(f"A{batch_start}:AB{batch_end}", batch)
                progress_bar.update(batch_end - batch_start + 1)

    def _insert(self, row: TrackerTabDataRow):
        self.rows[self.next_empty_row] = row
        self.next_empty_row += 1

    def add(self, transaction: OptionTransaction):
        self._insert(TrackerTabDataRow.from_transaction(transaction))

    def _fetch_rows_if_needed(self, product_key: str):
        """
        Fetch all rows associated with product_key and materialize them in the
        rows dict.
        """
        row_nums_to_fetch = self._product_key_to_row_num_index.get(product_key)
        if row_nums_to_fetch is None:
            return

        for row_num in row_nums_to_fetch:
            assert row_num <= self._last_remote_row_with_data
            assert isinstance(row_num, int)
            data = self._tracker.get_row(row_num)
            self.rows[row_num] = TrackerTabDataRow.from_tracker_data(data)

    def _find_rows_for(
        self, transaction: OptionTransaction
    ) -> list[tuple[int, TrackerTabDataRow]]:
        product_key = transaction.key

        self._fetch_rows_if_needed(product_key)

        matching_rows = [
            (row_num, row)
            for row_num, row in self.rows.items()
            if (row.product_key == product_key and row.close_reason == "")
        ]
        if len(matching_rows) == 0:
            return []

        matching_rows.sort(key=lambda item: item[0])  # row_num
        return matching_rows

    def _update_row(
        self,
        row: TrackerTabDataRow,
        num_contracts_covered: Decimal,
        transaction: OptionTransaction,
        category: TransactionCategory,
    ):
        num_contracts = Decimal(row.num_contracts)
        if num_contracts != num_contracts_covered:
            # The > case should have been handled before we got here
            assert num_contracts_covered < num_contracts

            # Transaction covers parts of the contract, split the row
            num_contracts_left = num_contracts - num_contracts_covered
            # Existing row contains the update
            row.num_contracts = f"{num_contracts_covered:, f}"
            # New row contains the remaining contracts
            new_row = TrackerTabDataRow.new_open(row)
            new_row.num_contracts = f"{num_contracts_left:,f}"
            new_row.roll_id = row.roll_id
            self._insert(new_row)

        # Update roll IDs
        if transaction.is_part_of_roll_order:
            assert transaction.order_id
            rolls = [
                roll_id.strip() for roll_id in row.roll_id.split(",") if roll_id.strip()
            ]
            rolls.append(transaction.order_id.strip())
            row.roll_id = ", ".join(rolls)

        # Update transaction IDs
        transaction_ids = [
            transaction_id.strip()
            for transaction_id in row.transactions.split(",")
            if transaction_id.strip()
        ]
        transaction_ids.append(transaction.id)
        row.transactions = ", ".join(transaction_ids)

        if (
            category == TransactionCategory.CLOSED_EARLY
            or category == TransactionCategory.ROLL
            or category == TransactionCategory.ASSIGNED
        ):
            row.closing_fees = f"{transaction.fee:,f}"
            row.exit_price = f"{transaction.price:,f}"
            row.close_date = transaction.date.strftime("%m/%d/%y")
            row.close_reason = category
        elif category == TransactionCategory.EXPIRED:
            row.closing_fees = "0"
            row.exit_price = "0"
            row.close_date = transaction.expiry_date
            row.close_reason = TransactionCategory.EXPIRED
        else:
            raise ValueError(
                f"Unexpected category {category} while processing {transaction}"
            )

        row.status = "Closed"

    def _update_multiple(
        self,
        transaction: OptionTransaction,
        category: TransactionCategory,
        row_tuples: list[tuple[int, TrackerTabDataRow]],
    ) -> bool:
        transaction_num_contracts = transaction.quantity

        # Check that we have enough contracts to cover the transaction
        total_contracts = sum(
            Decimal(row_tuple[1].num_contracts) for row_tuple in row_tuples
        )
        if transaction_num_contracts > total_contracts:
            logger.warning(
                f"{transaction} covers more contracts than there are in the data set"
            )
            return False

        num_contracts_left = transaction_num_contracts
        for row_tuple in row_tuples:
            row = row_tuple[1]
            num_contracts = Decimal(row.num_contracts)
            num_contracts_covered = (
                num_contracts
                if num_contracts_left >= num_contracts
                else num_contracts - num_contracts_left
            )

            self._update_row(row, num_contracts_covered, transaction, category)

            num_contracts_left -= num_contracts_covered
            if num_contracts_left == 0:
                break

        return True

    def update(
        self, transaction: OptionTransaction, category: TransactionCategory
    ) -> bool:
        rows = self._find_rows_for(transaction)

        if len(rows) == 0:
            logger.debug(f"Didn't find any rows for {transaction}")
            return False

        return self._update_multiple(transaction, category, rows)

    def __iter__(self):
        yield from sorted(self.rows.items(), key=lambda item: item[0])  # row_num


class OpenPositionsTab:
    """
    Table Headers
    =============
    [A] Product
    [B] Cost Basis
    [C] Extrinsic
    [D] Captured%
    [E] Remaining Return
    [F] Mark
    [G] Days Left
    [H] Intrinsic
    [I] Updated On
    """

    _gtab: PygsheetsWorksheet

    _LABEL = "Open Positions"

    def __init__(self, gsheet: PygsheetsSpreadsheet):
        self._gtab = gsheet.worksheet_by_title(OpenPositionsTab._LABEL)

    def update_tab(self, positions: list[OptionPosition]):
        _NUM_HEADER_ROWS = 1
        _FIRST_DATA_ROW = _NUM_HEADER_ROWS + 1

        if len(positions) == 0:
            return

        positions.sort(key=lambda v: v.remaining_annualized)

        current_row = _FIRST_DATA_ROW
        batch = []
        for position in positions:
            if position.quote is not None:
                batch.append(
                    [
                        position.product_key,
                        f"{position.cost_basis:f}",
                        f"=MAX(0, F{current_row}-H{current_row})",
                        (f"=MAX(0, B{current_row} - F{current_row}) / B{current_row}"),
                        (
                            f"=(MIN(F{current_row}, B{current_row}) / "
                            f"({position.strike_price:f} * G{current_row})) "
                            f"* 365"
                        ),
                        f"{position.quote.mark:f}",
                        position.quote.days_to_expiration,
                        f"{position.quote.intrinsic:f}",
                        position.quote.date.isoformat(),
                    ]
                )
                current_row += 1

        if len(batch) > 0:
            self._gtab.update_values(f"A{_FIRST_DATA_ROW}:", batch)
            if self._gtab.rows >= current_row:
                self._gtab.clear(start=f"A{current_row}")


class Worksheet:
    _client: PygsheetsClient
    _gsheet: PygsheetsSpreadsheet

    def __init__(self, google_sheet_id: str):
        self._client = pygsheets.authorize(
            client_secret="credentials.json",
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )

        self._gsheet = self._client.open_by_key(google_sheet_id)

    def open_tracker_tab(self) -> TrackerTab:
        return TrackerTab(self._gsheet)

    def open_positions_tab(self) -> OpenPositionsTab:
        return OpenPositionsTab(self._gsheet)
