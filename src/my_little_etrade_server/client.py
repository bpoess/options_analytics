"""Async gRPC client for the E*Trade proxy server."""

from collections.abc import AsyncIterator
from typing import Self

import grpc

from my_little_etrade_server.generated import (
    my_little_etrade_server_pb2 as pb,
)
from my_little_etrade_server.generated import (
    my_little_etrade_server_pb2_grpc as pb_grpc,
)


class ProxyClient:
    """Async gRPC client for the E*Trade proxy server."""

    def __init__(self, target: str = "localhost:38710"):
        self._target = target
        self._channel: grpc.aio.Channel | None = None
        self._stub: pb_grpc.ProxyServiceStub | None = None

    def _ensure_connected(self) -> pb_grpc.ProxyServiceStub:
        if self._stub is None:
            raise RuntimeError("ProxyClient must be used as an async context manager")
        return self._stub

    async def __aenter__(self) -> Self:
        self._channel = grpc.aio.insecure_channel(self._target)
        self._stub = pb_grpc.ProxyServiceStub(self._channel)
        return self

    async def __aexit__(self, *_exc: object) -> None:
        if self._channel:
            await self._channel.close()
            self._channel = None
            self._stub = None

    # --- Unary RPCs ---

    async def get_authentication_status(
        self,
    ) -> pb.GetAuthenticationStatusResponse:
        stub = self._ensure_connected()
        return await stub.GetAuthenticationStatus(pb.GetAuthenticationStatusRequest())

    async def get_authorization_url(self) -> pb.GetAuthorizationUrlResponse:
        stub = self._ensure_connected()
        return await stub.GetAuthorizationUrl(pb.GetAuthorizationUrlRequest())

    async def complete_authorization(
        self, verification_code: str
    ) -> pb.CompleteAuthorizationResponse:
        stub = self._ensure_connected()
        return await stub.CompleteAuthorization(
            pb.CompleteAuthorizationRequest(verification_code=verification_code)
        )

    async def list_accounts(self) -> pb.ListAccountsResponse:
        stub = self._ensure_connected()
        return await stub.ListAccounts(pb.ListAccountsRequest())

    async def get_order_details(self, details_url: str) -> pb.GetOrderDetailsResponse:
        stub = self._ensure_connected()
        return await stub.GetOrderDetails(
            pb.GetOrderDetailsRequest(details_url=details_url)
        )

    async def get_transaction_details(
        self, account_id_key: str, transaction_id: str
    ) -> pb.GetTransactionDetailsResponse:
        stub = self._ensure_connected()
        return await stub.GetTransactionDetails(
            pb.GetTransactionDetailsRequest(
                account_id_key=account_id_key, transaction_id=transaction_id
            )
        )

    async def get_option_chains(
        self,
        symbol: str,
        *,
        expiry_year: int | None = None,
        expiry_month: int | None = None,
        expiry_day: int | None = None,
        strike_price_near: int | None = None,
        no_of_strikes: int | None = None,
        include_weekly: bool | None = None,
        skip_adjusted: bool | None = None,
        option_category: str | None = None,
        chain_type: str | None = None,
        price_type: str | None = None,
    ) -> pb.GetOptionChainsResponse:
        stub = self._ensure_connected()
        request = pb.GetOptionChainsRequest(symbol=symbol)
        if expiry_year is not None:
            request.expiry_year = expiry_year
        if expiry_month is not None:
            request.expiry_month = expiry_month
        if expiry_day is not None:
            request.expiry_day = expiry_day
        if strike_price_near is not None:
            request.strike_price_near = strike_price_near
        if no_of_strikes is not None:
            request.no_of_strikes = no_of_strikes
        if include_weekly is not None:
            request.include_weekly = include_weekly
        if skip_adjusted is not None:
            request.skip_adjusted = skip_adjusted
        if option_category is not None:
            request.option_category = option_category
        if chain_type is not None:
            request.chain_type = chain_type
        if price_type is not None:
            request.price_type = price_type
        return await stub.GetOptionChains(request)

    async def get_option_expire_dates(
        self, symbol: str, *, expiry_type: str | None = None
    ) -> pb.GetOptionExpireDatesResponse:
        stub = self._ensure_connected()
        request = pb.GetOptionExpireDatesRequest(symbol=symbol)
        if expiry_type is not None:
            request.expiry_type = expiry_type
        return await stub.GetOptionExpireDates(request)

    # --- Streaming RPCs ---

    async def iter_positions(
        self, account_id_key: str, *, view: str | None = None
    ) -> AsyncIterator[pb.Position]:
        stub = self._ensure_connected()
        request = pb.ListPositionsRequest(account_id_key=account_id_key)
        if view is not None:
            request.view = view
        async for position in stub.ListPositions(request):
            yield position

    async def list_positions(
        self, account_id_key: str, *, view: str | None = None
    ) -> list[pb.Position]:
        return [p async for p in self.iter_positions(account_id_key, view=view)]

    async def iter_quotes(
        self, symbols: list[str], *, detail_flag: str | None = None
    ) -> AsyncIterator[pb.Quote]:
        stub = self._ensure_connected()
        request = pb.ListQuotesRequest(symbols=symbols)
        if detail_flag is not None:
            request.detail_flag = detail_flag
        async for quote in stub.ListQuotes(request):
            yield quote

    async def list_quotes(
        self, symbols: list[str], *, detail_flag: str | None = None
    ) -> list[pb.Quote]:
        return [q async for q in self.iter_quotes(symbols, detail_flag=detail_flag)]

    async def iter_orders(
        self,
        account_id_key: str,
        start_date: str,
        end_date: str,
        *,
        status: str | None = None,
    ) -> AsyncIterator[pb.Order]:
        stub = self._ensure_connected()
        request = pb.ListOrdersRequest(
            account_id_key=account_id_key,
            start_date=start_date,
            end_date=end_date,
        )
        if status is not None:
            request.status = status
        async for order in stub.ListOrders(request):
            yield order

    async def list_orders(
        self,
        account_id_key: str,
        start_date: str,
        end_date: str,
        *,
        status: str | None = None,
    ) -> list[pb.Order]:
        return [
            o
            async for o in self.iter_orders(
                account_id_key, start_date, end_date, status=status
            )
        ]

    async def iter_transactions(
        self, account_id_key: str, start_date: str, end_date: str
    ) -> AsyncIterator[pb.Transaction]:
        stub = self._ensure_connected()
        request = pb.ListTransactionsRequest(
            account_id_key=account_id_key,
            start_date=start_date,
            end_date=end_date,
        )
        async for transaction in stub.ListTransactions(request):
            yield transaction

    async def list_transactions(
        self, account_id_key: str, start_date: str, end_date: str
    ) -> list[pb.Transaction]:
        return [
            t
            async for t in self.iter_transactions(account_id_key, start_date, end_date)
        ]
