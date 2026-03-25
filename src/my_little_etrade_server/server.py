"""Async gRPC server exposing the E*Trade client API."""

from multiprocessing import freeze_support

if __name__ == "__main__":
    freeze_support()

import argparse
import asyncio
import json
import logging
import logging.handlers
import os
import sys
from typing import Any

import grpc
import httpx
from grpc import aio
from grpc_reflection.v1alpha import reflection

from etrade_client.async_client import AsyncETradeClient
from etrade_client.exceptions import AuthenticationRequired, Timeout
from my_little_etrade_server.converters import (
    ConversionError,
    dict_to_account,
    dict_to_option_chain_response,
    dict_to_option_expire_dates_response,
    dict_to_order,
    dict_to_position,
    dict_to_quote,
    dict_to_transaction,
)
from my_little_etrade_server.generated import (
    my_little_etrade_server_pb2,
    my_little_etrade_server_pb2_grpc,
)
from options_analytics.config import Config

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
    parser = argparse.ArgumentParser(
        description="A useful little server for interfacing with the E*Trade API"
    )
    parser.add_argument(
        "--loglevel",
        default="WARNING",
        help="Console log verbosity (DEBUG, INFO, WARNING, ERROR)",
    )
    parser.add_argument(
        "--logfile",
        default="my_little_etrade_server.log",
        help="Log file path (always logs at DEBUG level)",
    )
    parser.add_argument(
        "--background",
        action="store_true",
        help="Run the server in the background",
    )
    return parser.parse_args()


class ProxyServicer(my_little_etrade_server_pb2_grpc.ProxyServiceServicer):
    """gRPC servicer that delegates to AsyncETradeClient."""

    def __init__(self, client: AsyncETradeClient):
        self._client = client

    async def _call(
        self,
        context: grpc.aio.ServicerContext,
        coro: Any,  # noqa: ANN401
    ) -> Any:  # noqa: ANN401
        """Await an async client call with standard error handling."""
        try:
            return await coro
        except AuthenticationRequired:
            logger.info("OAuth session expired or revoked")
            await context.abort(
                grpc.StatusCode.UNAUTHENTICATED,
                "OAuth session expired or revoked",
            )
        except Timeout as err:
            logger.info(f"Timeout waiting for request to complete {err}")
            await context.abort(
                grpc.StatusCode.DEADLINE_EXCEEDED,
                "Request to E*Trade backend timed out",
            )
        except httpx.HTTPStatusError as err:
            logger.error(f"E*Trade API error: {err}")
            await context.abort(
                grpc.StatusCode.UNAVAILABLE,
                "E*Trade API request failed",
            )
        except Exception:
            logger.exception("Unexpected error calling E*Trade API")
            await context.abort(
                grpc.StatusCode.INTERNAL,
                "Unexpected error calling E*Trade API",
            )

    async def _abort_conversion_error(
        self,
        context: grpc.aio.ServicerContext,
        err: ConversionError,
    ) -> None:
        """Log full raw data and abort with INTERNAL status."""
        logger.error(
            f"Failed to convert {err.entity_type}: {err.cause}\n"
            f"Raw data: {err.raw_data}"
        )
        await context.abort(
            grpc.StatusCode.DATA_LOSS,
            f"Failed to process {err.entity_type} data from E*Trade API",
        )

    async def GetAuthenticationStatus(
        self,
        request: my_little_etrade_server_pb2.GetAuthenticationStatusRequest,
        context: grpc.aio.ServicerContext,
    ) -> my_little_etrade_server_pb2.GetAuthenticationStatusResponse:
        logger.info("GetAuthenticationStatus called")
        is_authenticated = await self._client.is_authenticated()
        return my_little_etrade_server_pb2.GetAuthenticationStatusResponse(
            is_authenticated=is_authenticated
        )

    async def GetAuthorizationUrl(
        self,
        request: my_little_etrade_server_pb2.GetAuthorizationUrlRequest,
        context: grpc.aio.ServicerContext,
    ) -> my_little_etrade_server_pb2.GetAuthorizationUrlResponse:
        logger.info("GetAuthorizationUrl called")
        url = await self._client.get_authorization_url()
        return my_little_etrade_server_pb2.GetAuthorizationUrlResponse(url=url)

    async def CompleteAuthorization(
        self,
        request: my_little_etrade_server_pb2.CompleteAuthorizationRequest,
        context: grpc.aio.ServicerContext,
    ) -> my_little_etrade_server_pb2.CompleteAuthorizationResponse:
        if not request.verification_code:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "verification_code is required",
            )
        logger.info("CompleteAuthorization called")
        await self._client.complete_authorization(request.verification_code)
        return my_little_etrade_server_pb2.CompleteAuthorizationResponse()

    async def ListAccounts(
        self,
        request: my_little_etrade_server_pb2.ListAccountsRequest,
        context: grpc.aio.ServicerContext,
    ) -> my_little_etrade_server_pb2.ListAccountsResponse:
        logger.info("ListAccounts called")
        accounts_data = await self._call(context, self._client.fetch_accounts())
        accounts = []
        for raw in accounts_data:
            logger.debug(f"Raw account data: {raw}")
            try:
                accounts.append(dict_to_account(raw))
            except ConversionError as err:
                await self._abort_conversion_error(context, err)
        return my_little_etrade_server_pb2.ListAccountsResponse(accounts=accounts)

    async def ListPositions(
        self,
        request: my_little_etrade_server_pb2.ListPositionsRequest,
        context: grpc.aio.ServicerContext,
    ):
        if not request.account_id_key:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "account_id_key is required",
            )
        logger.info(f"ListPositions called for {request.account_id_key}")
        view = request.view if request.HasField("view") else None
        positions_data = await self._call(
            context, self._client.fetch_portfolio(request.account_id_key, view)
        )
        for raw in positions_data:
            logger.debug(f"Raw Position JSON: {json.dumps(raw)}")
            try:
                yield dict_to_position(raw)
            except ConversionError as err:
                await self._abort_conversion_error(context, err)

    async def ListQuotes(
        self,
        request: my_little_etrade_server_pb2.ListQuotesRequest,
        context: grpc.aio.ServicerContext,
    ):
        if not request.symbols:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "symbols is required",
            )
        logger.debug(f"ListQuotes called for {len(request.symbols)} symbols")
        detail_flag = request.detail_flag if request.HasField("detail_flag") else None
        try:
            async for raw in self._client.stream_quotes_for(
                list(request.symbols), detail_flag
            ):
                try:
                    yield dict_to_quote(raw)
                except ConversionError as err:
                    await self._abort_conversion_error(context, err)
        except AuthenticationRequired:
            logger.info("OAuth session expired or revoked")
            await context.abort(
                grpc.StatusCode.UNAUTHENTICATED,
                "OAuth session expired or revoked",
            )
        except Timeout as err:
            logger.info(f"Timeout waiting for request to complete {err}")
            await context.abort(
                grpc.StatusCode.DEADLINE_EXCEEDED,
                "Request to E*Trade backend timed out",
            )
        except httpx.HTTPStatusError as err:
            logger.error(f"E*Trade API error: {err}")
            await context.abort(
                grpc.StatusCode.UNAVAILABLE,
                "E*Trade API request failed",
            )
        except Exception:
            logger.exception("Unexpected error calling E*Trade API")
            await context.abort(
                grpc.StatusCode.INTERNAL,
                "Unexpected error calling E*Trade API",
            )

    async def ListOrders(
        self,
        request: my_little_etrade_server_pb2.ListOrdersRequest,
        context: grpc.aio.ServicerContext,
    ):
        if not request.account_id_key:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "account_id_key is required",
            )
        if not request.start_date or not request.end_date:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "start_date and end_date are required",
            )
        logger.info(f"ListOrders called for {request.account_id_key}")
        status = request.status if request.HasField("status") else None
        orders_data = await self._call(
            context,
            self._client.fetch_order_list(
                request.account_id_key,
                request.start_date,
                request.end_date,
                status,
            ),
        )
        for raw in orders_data:
            try:
                yield dict_to_order(raw)
            except ConversionError as err:
                await self._abort_conversion_error(context, err)

    async def GetOrderDetails(
        self,
        request: my_little_etrade_server_pb2.GetOrderDetailsRequest,
        context: grpc.aio.ServicerContext,
    ) -> my_little_etrade_server_pb2.GetOrderDetailsResponse:
        if not request.details_url:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "details_url is required",
            )
        logger.info("GetOrderDetails called")
        order_dict = {"details": request.details_url}
        response_data = await self._call(
            context, self._client.fetch_order_details(order_dict)
        )
        orders = []
        if response_data and "Order" in response_data:
            for raw in response_data["Order"]:
                try:
                    orders.append(dict_to_order(raw))
                except ConversionError as err:
                    await self._abort_conversion_error(context, err)
        return my_little_etrade_server_pb2.GetOrderDetailsResponse(orders=orders)

    async def ListTransactions(
        self,
        request: my_little_etrade_server_pb2.ListTransactionsRequest,
        context: grpc.aio.ServicerContext,
    ):
        if not request.account_id_key:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "account_id_key is required",
            )
        if not request.start_date or not request.end_date:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "start_date and end_date are required",
            )
        logger.info(f"ListTransactions called for {request.account_id_key}")
        transactions_data = await self._call(
            context,
            self._client.fetch_transactions(
                request.account_id_key,
                request.start_date,
                request.end_date,
            ),
        )
        for raw in transactions_data:
            try:
                yield dict_to_transaction(raw)
            except ConversionError as err:
                await self._abort_conversion_error(context, err)

    async def GetTransactionDetails(
        self,
        request: my_little_etrade_server_pb2.GetTransactionDetailsRequest,
        context: grpc.aio.ServicerContext,
    ) -> my_little_etrade_server_pb2.GetTransactionDetailsResponse:
        if not request.account_id_key:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "account_id_key is required",
            )
        if not request.transaction_id:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "transaction_id is required",
            )
        logger.info(f"GetTransactionDetails called for {request.transaction_id}")
        txn_data = await self._call(
            context,
            self._client.fetch_transaction_details(
                request.account_id_key,
                request.transaction_id,
            ),
        )
        try:
            transaction = dict_to_transaction(txn_data)
        except ConversionError as err:
            await self._abort_conversion_error(context, err)
            return my_little_etrade_server_pb2.GetTransactionDetailsResponse()
        return my_little_etrade_server_pb2.GetTransactionDetailsResponse(
            transaction=transaction
        )

    async def GetOptionChains(
        self,
        request: my_little_etrade_server_pb2.GetOptionChainsRequest,
        context: grpc.aio.ServicerContext,
    ) -> my_little_etrade_server_pb2.GetOptionChainsResponse:
        if not request.symbol:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "symbol is required",
            )
        logger.info(f"GetOptionChains called for {request.symbol}")
        kwargs: dict[str, Any] = {}
        for field in (
            "expiry_year",
            "expiry_month",
            "expiry_day",
            "strike_price_near",
            "no_of_strikes",
            "include_weekly",
            "skip_adjusted",
            "option_category",
            "chain_type",
            "price_type",
        ):
            if request.HasField(field):
                kwargs[field] = getattr(request, field)
        response_data = await self._call(
            context,
            self._client.fetch_option_chains(request.symbol, **kwargs),
        )
        logger.debug(f"Raw OptionChains JSON: {json.dumps(response_data)}")
        try:
            response = dict_to_option_chain_response(response_data)
        except ConversionError as err:
            await self._abort_conversion_error(context, err)
            return my_little_etrade_server_pb2.GetOptionChainsResponse()
        return response

    async def GetOptionExpireDates(
        self,
        request: my_little_etrade_server_pb2.GetOptionExpireDatesRequest,
        context: grpc.aio.ServicerContext,
    ) -> my_little_etrade_server_pb2.GetOptionExpireDatesResponse:
        if not request.symbol:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "symbol is required",
            )
        logger.info(f"GetOptionExpireDates called for {request.symbol}")
        kwargs: dict[str, Any] = {}
        if request.HasField("expiry_type"):
            kwargs["expiry_type"] = request.expiry_type
        response_data = await self._call(
            context,
            self._client.fetch_option_expire_dates(request.symbol, **kwargs),
        )
        logger.debug(f"Raw OptionExpireDates JSON: {json.dumps(response_data)}")
        try:
            response = dict_to_option_expire_dates_response(response_data)
        except ConversionError as err:
            await self._abort_conversion_error(context, err)
            return my_little_etrade_server_pb2.GetOptionExpireDatesResponse()
        return response


def daemonize(pid_file: str = "my_little_etrade_server.pid") -> None:
    """Double-fork to daemonize the process."""
    # First fork
    pid = os.fork()
    if pid > 0:
        sys.exit(0)

    os.setsid()

    # Second fork
    pid = os.fork()
    if pid > 0:
        sys.exit(0)

    # Redirect standard file descriptors to /dev/null
    devnull = os.open(os.devnull, os.O_RDWR)
    os.dup2(devnull, sys.stdin.fileno())
    os.dup2(devnull, sys.stdout.fileno())
    os.dup2(devnull, sys.stderr.fileno())
    os.close(devnull)

    # Write PID file
    with open(pid_file, "w") as f:
        f.write(str(os.getpid()))


async def serve() -> None:
    config = Config.from_file("config.toml")
    port = config.etrade.proxy.port

    client = AsyncETradeClient(config.etrade.key.api, config.etrade.key.secret)
    servicer = ProxyServicer(client)

    server = aio.server()
    my_little_etrade_server_pb2_grpc.add_ProxyServiceServicer_to_server(
        servicer, server
    )
    service_names = (
        my_little_etrade_server_pb2.DESCRIPTOR.services_by_name[
            "ProxyService"
        ].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(service_names, server)
    listen_addr = f"[::]:{port}"
    server.add_insecure_port(listen_addr)

    logger.info(f"Starting gRPC server on {listen_addr}")
    await server.start()
    logger.info("Server started, waiting for connections...")

    try:
        await server.wait_for_termination()
    except asyncio.CancelledError:
        logger.info("Shutting down server...")
        await server.stop(grace=5)


def main() -> int:
    args = parse_args()

    if args.background:
        daemonize()

    configure_logging(args)
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        logger.info("Server stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
