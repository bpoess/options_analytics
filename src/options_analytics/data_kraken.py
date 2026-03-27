import argparse
import asyncio
import datetime
import logging
import logging.handlers
import queue
import threading
import time
import tomllib
from datetime import date as Date
from datetime import datetime as DateTime
from datetime import timedelta as TimeDelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from google.protobuf.internal.encoder import _VarintBytes  # pyright: ignore
from pydantic import BaseModel, ValidationError

from my_little_etrade_server.client import ProxyClient
from my_little_etrade_server.generated import (
    my_little_etrade_server_pb2 as etrade_pb,
)

NEW_YORK_TIME = ZoneInfo("America/New_York")

logger: logging.Logger


def configure_logging(args: argparse.Namespace) -> None:
    root_logger = logging.getLogger()
    formatter = logging.Formatter(
        "%(asctime)s %(name)s %(filename)s:%(lineno)d "
        "%(funcName)s %(levelname)s - %(message)s"
    )

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
    parser = argparse.ArgumentParser(description="Data sink for the E*Trade API")
    parser.add_argument(
        "--loglevel",
        default="WARNING",
        help="Console log verbosity (DEBUG, INFO, WARNING, ERROR)",
    )
    parser.add_argument(
        "--logfile",
        default="data_kraken.log",
        help="Log file path (always logs at DEBUG level)",
    )
    parser.add_argument(
        "--proxy-port",
        default=38710,
        help="Proxy server port to connect to",
    )
    parser.add_argument("--datadir", default=None, help="Directory for data files.")
    parser.add_argument("--no-symbols", action="store_false", dest="record_symbols")
    parser.add_argument("--no-options", action="store_false", dest="record_options")

    return parser.parse_args()


class ConfigSymbols(BaseModel):
    symbols: list[str]


class PortfolioConfig(BaseModel):
    version: int
    equity: ConfigSymbols | None
    options: ConfigSymbols | None
    index: ConfigSymbols | None


def load_portfolio_config() -> PortfolioConfig:
    _CONFIG_FILE_LOCATION = "kraken.toml"

    try:
        with open(_CONFIG_FILE_LOCATION, "rb") as f:
            data = tomllib.load(f)

        return PortfolioConfig.model_validate(data)

    except FileNotFoundError:
        logger.error(f"Config not found at {_CONFIG_FILE_LOCATION}")
        raise
    except tomllib.TOMLDecodeError as e:
        logger.error(f"Unable to parse configuration: {e}")
        raise
    except ValidationError as e:
        logger.error(f"Malformed configuration: {e.errors(), e.json()}")
        raise
    except Exception as e:
        logger.error(f"Unable to parse configuration: {e}")
        raise


class DiskWriterMessage:
    exit: bool  # Set to true if worker should flush and exit
    symbol_quotes: list[etrade_pb.Quote] | None
    option_quotes: list[etrade_pb.GetOptionChainsResponse] | None

    def __init__(
        self,
        exit: bool = False,
        symbol_quotes: list[etrade_pb.Quote] | None = None,
        option_quotes: list[etrade_pb.GetOptionChainsResponse] | None = None,
    ):
        self.exit = exit
        self.symbol_quotes = symbol_quotes
        self.option_quotes = option_quotes


class DiskWriter(threading.Thread):
    _should_exit: bool
    _working_directory: Path
    message_queue: queue.Queue[DiskWriterMessage]

    def __init__(self, datadir: str | None):
        self._should_exit = False
        self.message_queue = queue.Queue()

        if datadir:
            self._working_directory = Path(datadir)
        else:
            self._working_directory = Path.cwd()

        super().__init__(daemon=True)

    def write_symbols(self, quotes: list[etrade_pb.Quote]):
        today = DateTime.now(NEW_YORK_TIME).date()
        filename = f"kraken.symbols.{today.strftime('%Y%m%d')}.pb.db"
        filepath = self._working_directory / filename
        with open(filepath, "ab") as f:
            for quote in quotes:
                f.write(_VarintBytes(quote.ByteSize()))
                f.write(quote.SerializeToString())

        logger.info("Flushed symbol quotes to disk")

    def write_option_quotes(self, quotes: list[etrade_pb.GetOptionChainsResponse]):
        today = DateTime.now(NEW_YORK_TIME).date()
        filename = f"kraken.options.{today.strftime('%Y%m%d')}.pb.db"
        filepath = self._working_directory / filename
        with open(filepath, "ab") as f:
            for quote in quotes:
                f.write(_VarintBytes(quote.ByteSize()))
                f.write(quote.SerializeToString())

        logger.info("Flushed option quotes to disk")

    def run(self):
        """Background thread that handles flushing IO"""

        while not self._should_exit:
            message = self.message_queue.get()
            if message.exit:
                logger.info("Exit message received")
                self._should_exit = True

            if message.symbol_quotes:
                self.write_symbols(message.symbol_quotes)

            if message.option_quotes:
                self.write_option_quotes(message.option_quotes)

            self.message_queue.task_done()

    def wait_to_finish(self):
        # Wait for all messages from other tasks to flush
        self.message_queue.join()
        # Signal exit and wait for the diskwriter thread to finish
        self.message_queue.put(DiskWriterMessage(exit=True))
        self.join()
        assert self.message_queue.empty()
        logger.info("Diskwriter complete.")


class MarketTime(DateTime):
    @classmethod
    def now(cls, tz=None):
        assert tz is None
        return super().now(NEW_YORK_TIME)

    @classmethod
    def combine(cls, date, time, tzinfo=None):
        assert tzinfo is None
        return super().combine(date, time, NEW_YORK_TIME)


class MarketHours:
    day: Date
    pre_market_open: MarketTime
    open: MarketTime
    close: MarketTime
    after_hours_close: MarketTime

    def __init__(self):
        now = MarketTime.now()

        if now.hour > 20:
            # Past market hours, add a day
            self.day = now.date() + TimeDelta(days=1)
        else:
            # Before or within market ours
            self.day = now.date()

        self.pre_market_open = MarketTime.combine(self.day, datetime.time(7, 0))
        self.open = MarketTime.combine(self.day, datetime.time(9, 30))
        self.close = MarketTime.combine(self.day, datetime.time(13, 0))
        self.after_hours_close = MarketTime.combine(self.day, datetime.time(20, 0))

    async def wait_until_pre_market_open(self):
        now = MarketTime.now()

        delta = self.pre_market_open - now
        sleep_time = max(0, delta.total_seconds())
        if sleep_time > 0:
            print(f"{delta} until pre-market open")
            await asyncio.sleep(sleep_time)

    async def wait_until_market_open(self):
        now = MarketTime.now()

        delta = self.open - now
        sleep_time = max(0, delta.total_seconds())
        if sleep_time > 0:
            print(f"{delta} until market open")
            await asyncio.sleep(sleep_time)


def expand_expiry_dates():
    _FRIDAY = 4

    today = MarketTime.now().date()
    if today.weekday() != _FRIDAY:
        delta_to_next_friday = (_FRIDAY - today.weekday() + 7) % 7
        friday = today + TimeDelta(days=delta_to_next_friday)
    else:
        friday = today

    dates = []
    # We are interested in weekly options for all dates up to 50 days out
    while friday - today < TimeDelta(days=50):
        dates.append(friday)
        friday += TimeDelta(days=7)

    return dates


def decimal_round_nearest(number, multiple) -> int:
    return int(
        (number / multiple).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * multiple
    )


async def options_gatherer(
    client: ProxyClient,
    symbols: list[str],
    message_queue: queue.Queue[DiskWriterMessage],
    market: MarketHours,
):
    # Make sure we are expanding options on market day
    print("options_gatherer: Waiting for market to open")
    await market.wait_until_market_open()

    expiry_dates = expand_expiry_dates()
    quotes = {}
    try:
        while True:
            num_high_at_zero = 0
            async for quote in client.iter_quotes(symbols, detail_flag="ALL"):
                symbol = quote.product.symbol
                quotes[symbol] = {
                    "high": decimal_round_nearest(Decimal(quote.all.high.value), 5),
                    "low": decimal_round_nearest(Decimal(quote.all.low.value), 5),
                }
                if quotes[symbol]["high"] == 0:
                    logger.info(
                        f"{symbol} "
                        f"{quote.all.high.value} {quotes[symbol]['high']} "
                        f"{quote.all.low.value} {quotes[symbol]['low']}"
                    )
                    num_high_at_zero += 1

            if num_high_at_zero <= 1:
                break

            await asyncio.sleep(300)

    except Exception as e:
        logger.info(f"Error while fetching quotes {e}")
        return

    option_symbols = []
    for symbol, values in quotes.items():
        high = values["high"]
        low = values["low"]
        print(
            f"{symbol} L {low:.0f} H {high:.0f}, "
            f"PUT {max(0, low - 30)} - {low + 10} "
            f"CALL {max(0, high - 10)} - {high + 30}"
        )

        for date in expiry_dates:
            for strike in range(max(0, low - 30), low + 10, 5):
                option_symbols.append(
                    f"{symbol}:{date.year}:{date.month}:{date.day}:PUT:{strike}"
                )

            for strike in range(max(0, high - 10), high + 30, 5):
                option_symbols.append(
                    f"{symbol}:{date.year}:{date.month}:{date.day}:CALL:{strike}"
                )

    print("options_gatherer: starting")

    _FLUSH_WHEN_AT = 2 * 1024 * 1024
    _MAX_ERRORS = 100
    _MAX_CONSECUTIVE_ERRORS = 30
    _FETCH_EVERY_SECONDS = 10

    num_errors = 0
    num_consecutive_errors = 0
    buffer = []
    buffer_size = 0
    try:
        now = MarketTime.now()
        while now <= market.close:
            start_time = time.monotonic()

            try:
                async for quote in client.iter_quotes(
                    option_symbols, detail_flag="ALL"
                ):
                    buffer.append(quote)
                    buffer_size += quote.ByteSize()

                num_consecutive_errors = 0
            except Exception as e:
                logger.info(f"Error while fetching option quotes {e}")
                num_errors += 1
                num_consecutive_errors += 1

            if buffer_size >= _FLUSH_WHEN_AT:
                logger.info("Flushing option quotes")
                message_queue.put(DiskWriterMessage(option_quotes=list(buffer)))
                buffer.clear()
                buffer_size = 0

            if (
                num_errors > _MAX_ERRORS
                or num_consecutive_errors > _MAX_CONSECUTIVE_ERRORS
            ):
                logger.error(
                    f"Too many errors: consecutive {num_consecutive_errors} "
                    f"total {num_errors}. Stopping symbol recording."
                )
                break

            end_time = time.monotonic()
            sleep_time = max(0, _FETCH_EVERY_SECONDS - (end_time - start_time))

            await asyncio.sleep(sleep_time)
            now = MarketTime.now()

    except asyncio.exceptions.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Fatal error while gathering options {e}")

    if buffer_size > 0:
        logger.info("Flushing remaining option quotes")
        message_queue.put(DiskWriterMessage(option_quotes=list(buffer)))
        buffer.clear()
        buffer_size = 0


def expand_portfolio_symbols(portfolio_config: PortfolioConfig) -> list[str]:
    """Create a list of all equity or index symbols to fetch quotes for."""
    symbols = []
    if portfolio_config.equity:
        symbols.extend(portfolio_config.equity.symbols)

    if portfolio_config.options:
        # We always fetch underlying data for options
        symbols.extend(portfolio_config.options.symbols)

    if portfolio_config.index:
        symbols.extend(portfolio_config.index.symbols)

    return symbols


async def symbol_gatherer(
    client: ProxyClient,
    symbols: list[str],
    message_queue: queue.Queue[DiskWriterMessage],
    market: MarketHours,
):
    _FLUSH_WHEN_AT = 2 * 1024 * 1024
    _MAX_ERRORS = 100
    _MAX_CONSECUTIVE_ERRORS = 30

    quotes: list[etrade_pb.Quote] = []
    buffer_size = 0
    num_errors = 0
    num_consecutive_errors = 0

    print("symbol_gatherer: Waiting for pre-market to open")
    await market.wait_until_pre_market_open()

    print("symbol_gatherer: starting")

    try:
        now = MarketTime.now()
        while now < market.after_hours_close:
            start_time = time.monotonic()

            try:
                async for quote in client.iter_quotes(symbols, detail_flag="ALL"):
                    quotes.append(quote)
                    buffer_size += quote.ByteSize()
                num_consecutive_errors = 0
            except Exception as e:
                logger.info(f"Error while fetching quotes {e}")
                num_errors += 1
                num_consecutive_errors += 1

            if buffer_size >= _FLUSH_WHEN_AT:
                logger.info("Flushing symbol quotes")
                message_queue.put(DiskWriterMessage(symbol_quotes=list(quotes)))
                quotes.clear()
                buffer_size = 0

            if (
                num_errors > _MAX_ERRORS
                or num_consecutive_errors > _MAX_CONSECUTIVE_ERRORS
            ):
                logger.error(
                    f"Too many errors: consecutive {num_consecutive_errors} "
                    f"total {num_errors}. Stopping symbol recording."
                )
                break

            end_time = time.monotonic()
            sleep_time = max(0, 1.0 - (end_time - start_time))

            await asyncio.sleep(sleep_time)
            now = MarketTime.now()

    except asyncio.exceptions.CancelledError:
        pass

    if buffer_size > 0:
        logger.info("Flushing remaining symbol quotes")
        message_queue.put(DiskWriterMessage(symbol_quotes=list(quotes)))
        quotes.clear()
        buffer_size = 0


async def gather(
    args: argparse.Namespace,
    portfolio_config: PortfolioConfig,
    message_queue: queue.Queue[DiskWriterMessage],
) -> None:

    market = MarketHours()
    print(
        f"[ET] Now: {MarketTime.now()} "
        f"Market {market.day} Pre Open {market.pre_market_open.strftime('%H:%M')} "
        f"Open {market.open.strftime('%H:%M')} "
        f"Close {market.close.strftime('%H:%M')} "
        f"After Hours Close {market.after_hours_close.strftime('%H:%M')} "
    )

    try:
        async with ProxyClient(target=f"localhost:{args.proxy_port}") as client:
            tasks = []
            if args.record_symbols:
                symbols_to_gather = expand_portfolio_symbols(portfolio_config)
                logger.info("Starting symbol recorder")
                tasks.append(
                    asyncio.create_task(
                        symbol_gatherer(
                            client, symbols_to_gather, message_queue, market
                        )
                    )
                )
            else:
                logger.info("Not recording symbols")

            if args.record_options and portfolio_config.options:
                logger.info("Starting options recorder")
                tasks.append(
                    asyncio.create_task(
                        options_gatherer(
                            client,
                            list(portfolio_config.options.symbols),
                            message_queue,
                            market,
                        )
                    )
                )
            else:
                logger.info("Not recording options")

            await asyncio.gather(*tasks)

        print("All tasks completed successfully. Exiting.")
    except asyncio.CancelledError:
        print("Interrupt received! Finishing pending tasks...")
        # Gather all pending tasks and wait for them to finish
        pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        if pending:
            await asyncio.gather(*pending)
        print("All tasks completed. Exiting.")
        pass


def main() -> int:
    args = parse_args()
    configure_logging(args)

    diskwriter = DiskWriter(args.datadir)
    diskwriter.start()

    try:
        portfolio_config = load_portfolio_config()
    except Exception:
        return 1

    try:
        asyncio.run(gather(args, portfolio_config, diskwriter.message_queue))
    except KeyboardInterrupt:
        pass

    print("Waiting for disk writer to finish")
    diskwriter.wait_to_finish()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
