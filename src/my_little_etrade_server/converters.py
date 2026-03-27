"""Pure functions converting E*Trade raw dicts to protobuf messages."""

from decimal import Decimal
from functools import wraps
from typing import Any

from google.protobuf.timestamp_pb2 import Timestamp
from google.type.decimal_pb2 import Decimal as DecimalPb

from my_little_etrade_server.generated import (
    my_little_etrade_server_pb2 as pb,
)


class ConversionError(Exception):
    """Raised when converting raw E*Trade data to protobuf fails."""

    def __init__(self, entity_type: str, raw_data: dict, cause: Exception):
        self.entity_type = entity_type
        self.raw_data = raw_data
        self.cause = cause
        super().__init__(f"Failed to convert {entity_type}: {cause}")


def _converts(entity_type: str):
    """Decorator that wraps converter exceptions in ConversionError."""

    def decorator(func):
        @wraps(func)
        def wrapper(d: dict, *args, **kwargs):
            try:
                return func(d, *args, **kwargs)
            except ConversionError:
                raise
            except Exception as err:
                raise ConversionError(entity_type, d, err) from err

        return wrapper

    return decorator


def ms_to_timestamp(epoch_ms: int | float) -> Timestamp:
    """Convert epoch milliseconds to a protobuf Timestamp."""
    ts = Timestamp()
    seconds = int(epoch_ms / 1000)
    nanos = int((epoch_ms % 1000) * 1_000_000)
    ts.seconds = seconds
    ts.nanos = nanos
    return ts


def s_to_timestamp(epoch_s: int | float) -> Timestamp:
    """Convert epoch seconds to a protobuf Timestamp."""
    ts = Timestamp()
    ts.seconds = int(epoch_s)
    return ts


def to_decimal(value: Any) -> DecimalPb:
    """Convert a numeric value to a google.type.Decimal message."""
    return DecimalPb(value=str(Decimal(str(value)).normalize()))


def dict_to_product_id(d: dict) -> pb.ProductId:
    return pb.ProductId(
        symbol=d["symbol"],
        type_code=d["typeCode"],
    )


def dict_to_product(d: dict) -> pb.Product:
    kwargs: dict[str, Any] = {
        "symbol": d["symbol"],
    }
    if "securityType" in d:
        kwargs["security_type"] = d["securityType"]
    if "securitySubType" in d:
        kwargs["security_sub_type"] = d["securitySubType"]
    if "callPut" in d:
        kwargs["call_put"] = d["callPut"]
    if "expiryYear" in d:
        kwargs["expiry_year"] = d["expiryYear"]
    if "expiryMonth" in d:
        kwargs["expiry_month"] = d["expiryMonth"]
    if "expiryDay" in d:
        kwargs["expiry_day"] = d["expiryDay"]
    if "strikePrice" in d:
        kwargs["strike_price"] = to_decimal(d["strikePrice"])
    if "expiryType" in d:
        kwargs["expiry_type"] = d["expiryType"]
    if "productId" in d:
        kwargs["product_id"] = dict_to_product_id(d["productId"])
    return pb.Product(**kwargs)


@_converts("account")
def dict_to_account(d: dict) -> pb.Account:
    kwargs: dict[str, Any] = {
        "account_id": d["accountId"],
        "account_id_key": d["accountIdKey"],
        "account_name": d["accountName"],
        "account_type": d["accountType"],
        "institution_type": d["institutionType"],
        "account_status": d["accountStatus"],
        "account_mode": d["accountMode"],
        "share_works_account": d["shareWorksAccount"],
        "fc_managed_mssb_closed_account": d["fcManagedMssbClosedAccount"],
    }
    if d.get("closedDate"):
        kwargs["closed_date"] = ms_to_timestamp(d["closedDate"])
    if "accountDesc" in d:
        kwargs["account_desc"] = d["accountDesc"]
    if "instNo" in d:
        kwargs["inst_no"] = d["instNo"]
    if "shareWorksSource" in d:
        kwargs["share_works_source"] = d["shareWorksSource"]
    return pb.Account(**kwargs)


def dict_to_position_complete_view(d: dict) -> pb.PositionCompleteView:
    kwargs: dict[str, Any] = {
        "adj_last_trade": to_decimal(d["adjLastTrade"]),
        "adj_prev_close": to_decimal(d["adjPrevClose"]),
        "adj_price": to_decimal(d["adjPrice"]),
        "annual_dividend": to_decimal(d["annualDividend"]),
        "ask": to_decimal(d["ask"]),
        "ask_size": d["askSize"],
        "base_symbol_and_price": d["baseSymbolAndPrice"],
        "beta": to_decimal(d["beta"]),
        "bid": to_decimal(d["bid"]),
        "bid_ask_spread": to_decimal(d["bidAskSpread"]),
        "bid_size": d["bidSize"],
        "change": to_decimal(d["change"]),
        "change_pct": to_decimal(d["changePct"]),
        "currency": d["currency"],
        "cusip": d["cusip"],
        "days_range": d["daysRange"],
        "deliverables_str": d["deliverablesStr"],
        "delta": to_decimal(d["delta"]),
        "delta52_wk_high": to_decimal(d["delta52WkHigh"]),
        "delta52_wk_low": to_decimal(d["delta52WkLow"]),
        "div_yield": to_decimal(d["divYield"]),
        "dividend": to_decimal(d["dividend"]),
        "eps": to_decimal(d["eps"]),
        "exchange": d["exchange"],
        "gamma": to_decimal(d["gamma"]),
        "intrinsic_value": to_decimal(d["intrinsicValue"]),
        "iv_pct": to_decimal(d["ivPct"]),
        "last_trade": to_decimal(d["lastTrade"]),
        "marginable": d["marginable"],
        "market_cap": to_decimal(d["marketCap"]),
        "open": to_decimal(d["open"]),
        "open_interest": to_decimal(d["openInterest"]),
        "option_multiplier": to_decimal(d["optionMultiplier"]),
        "options_adjusted_flag": d["optionsAdjustedFlag"],
        "pe_ratio": to_decimal(d["peRatio"]),
        "premium": to_decimal(d["premium"]),
        "prev_close": to_decimal(d["prevClose"]),
        "price": to_decimal(d["price"]),
        "price_adjusted_flag": d["priceAdjustedFlag"],
        "quote_status": d["quoteStatus"],
        "rho": to_decimal(d["rho"]),
        "sv10_days_avg": to_decimal(d["sv10DaysAvg"]),
        "sv1_mon_avg": to_decimal(d["sv1MonAvg"]),
        "sv20_days_avg": to_decimal(d["sv20DaysAvg"]),
        "sv2_mon_avg": to_decimal(d["sv2MonAvg"]),
        "sv3_mon_avg": to_decimal(d["sv3MonAvg"]),
        "sv4_mon_avg": to_decimal(d["sv4MonAvg"]),
        "sv6_mon_avg": to_decimal(d["sv6MonAvg"]),
        "symbol_description": d["symbolDescription"],
        "ten_day_volume": d["tenDayVolume"],
        "theta": to_decimal(d["theta"]),
        "vega": to_decimal(d["vega"]),
        "volume": to_decimal(d["volume"]),
        "week52_high": to_decimal(d["week52High"]),
        "week52_low": to_decimal(d["week52Low"]),
        "week52_range": d["week52Range"],
    }
    if d.get("lastTradeTime"):
        kwargs["last_trade_time"] = s_to_timestamp(d["lastTradeTime"])
    if "daysToExpiration" in d:
        kwargs["days_to_expiration"] = d["daysToExpiration"]
    if d.get("divPayDate"):
        kwargs["div_pay_date"] = ms_to_timestamp(d["divPayDate"])
    if "estEarnings" in d:
        kwargs["est_earnings"] = to_decimal(d["estEarnings"])
    if d.get("exDividendDate"):
        kwargs["ex_dividend_date"] = ms_to_timestamp(d["exDividendDate"])
    if "perform12Month" in d:
        kwargs["perform12_month"] = to_decimal(d["perform12Month"])
    if "perform1Month" in d:
        kwargs["perform1_month"] = to_decimal(d["perform1Month"])
    if "perform3Month" in d:
        kwargs["perform3_month"] = to_decimal(d["perform3Month"])
    if "perform6Month" in d:
        kwargs["perform6_month"] = to_decimal(d["perform6Month"])
    if "prevDayVolume" in d:
        kwargs["prev_day_volume"] = to_decimal(d["prevDayVolume"])
    return pb.PositionCompleteView(**kwargs)


def dict_to_position_quick_view(d: dict) -> pb.PositionQuickView:
    kwargs: dict[str, Any] = {
        "change": to_decimal(d["change"]),
        "change_pct": to_decimal(d["changePct"]),
        "last_trade": to_decimal(d["lastTrade"]),
        "quote_status": d["quoteStatus"],
        "volume": to_decimal(d["volume"]),
    }
    if d.get("lastTradeTime"):
        kwargs["last_trade_time"] = s_to_timestamp(d["lastTradeTime"])
    return pb.PositionQuickView(**kwargs)


@_converts("position")
def dict_to_position(d: dict) -> pb.Position:
    kwargs: dict[str, Any] = {
        "adj_prev_close": to_decimal(d["adjPrevClose"]),
        "commissions": to_decimal(d["commissions"]),
        "cost_per_share": to_decimal(d["costPerShare"]),
        "days_gain": to_decimal(d["daysGain"]),
        "days_gain_pct": to_decimal(d["daysGainPct"]),
        "lot_detail_url": d["lotsDetails"],
        "market_value": to_decimal(d["marketValue"]),
        "other_fees": to_decimal(d["otherFees"]),
        "pct_of_portfolio": to_decimal(d["pctOfPortfolio"]),
        "position_id": str(d["positionId"]),
        "position_indicator": d["positionIndicator"],
        "position_type": d["positionType"],
        "price_paid": to_decimal(d["pricePaid"]),
        "product": dict_to_product(d["Product"]),
        "quantity": to_decimal(d["quantity"]),
        "quote_detail_url": d["quoteDetails"],
        "symbol_description": d["symbolDescription"],
        "today_commissions": to_decimal(d["todayCommissions"]),
        "today_fees": to_decimal(d["todayFees"]),
        "today_price_paid": to_decimal(d["todayPricePaid"]),
        "today_quantity": to_decimal(d["todayQuantity"]),
        "total_cost": to_decimal(d["totalCost"]),
        "total_gain": to_decimal(d["totalGain"]),
        "total_gain_pct": to_decimal(d["totalGainPct"]),
    }
    if d.get("dateAcquired"):
        kwargs["date_acquired"] = ms_to_timestamp(d["dateAcquired"])
    if "Complete" in d:
        kwargs["complete"] = dict_to_position_complete_view(d["Complete"])
    if "osiKey" in d:
        kwargs["osi_key"] = d["osiKey"]
    if "Quick" in d:
        kwargs["quick"] = dict_to_position_quick_view(d["Quick"])
    return pb.Position(**kwargs)


def dict_to_option_greeks(d: dict) -> pb.OptionGreeks:
    return pb.OptionGreeks(
        current_value=d["currentValue"],
        delta=to_decimal(d["delta"]),
        gamma=to_decimal(d["gamma"]),
        iv=to_decimal(d["iv"]),
        rho=to_decimal(d["rho"]),
        theta=to_decimal(d["theta"]),
        vega=to_decimal(d["vega"]),
    )


def dict_to_quote_option_detail(d: dict) -> pb.QuoteOptionDetail:
    kwargs: dict[str, Any] = {
        "ask": to_decimal(d["ask"]),
        "ask_size": d["askSize"],
        "bid": to_decimal(d["bid"]),
        "bid_size": d["bidSize"],
        "company_name": d["companyName"],
        "contract_size": to_decimal(d["contractSize"]),
        "days_to_expiration": d["daysToExpiration"],
        "intrinsic_value": to_decimal(d["intrinsicValue"]),
        "last_trade": to_decimal(d["lastTrade"]),
        "open_interest": d["openInterest"],
        "option_multiplier": to_decimal(d["optionMultiplier"]),
        "option_previous_ask_price": to_decimal(d["optionPreviousAskPrice"]),
        "option_previous_bid_price": to_decimal(d["optionPreviousBidPrice"]),
        "osi_key": d["osiKey"],
        "symbol_description": d["symbolDescription"],
        "time_premium": to_decimal(d["timePremium"]),
    }
    if "OptionGreeks" in d:
        kwargs["option_greeks"] = dict_to_option_greeks(d["OptionGreeks"])
    return pb.QuoteOptionDetail(**kwargs)


def dict_to_quote_all_detail(d: dict) -> pb.QuoteAllDetail:
    kwargs: dict[str, Any] = {
        "adjusted_flag": d["adjustedFlag"],
        "ask": to_decimal(d["ask"]),
        "ask_size": d["askSize"],
        "ask_time": d["askTime"],
        "average_volume": to_decimal(d["averageVolume"]),
        "beta": to_decimal(d["beta"]),
        "bid": to_decimal(d["bid"]),
        "bid_exchange": d["bidExchange"],
        "bid_size": d["bidSize"],
        "bid_time": d["bidTime"],
        "cash_deliverable": to_decimal(d["cashDeliverable"]),
        "change_close": to_decimal(d["changeClose"]),
        "change_close_percentage": to_decimal(d["changeClosePercentage"]),
        "company_name": d["companyName"],
        "contract_size": to_decimal(d["contractSize"]),
        "days_to_expiration": d["daysToExpiration"],
        "declared_dividend": to_decimal(d["declaredDividend"]),
        "dir_last": d["dirLast"],
        "dividend": to_decimal(d["dividend"]),
        "eps": to_decimal(d["eps"]),
        "est_earnings": to_decimal(d["estEarnings"]),
        "high": to_decimal(d["high"]),
        "high52": to_decimal(d["high52"]),
        "intrinsic_value": to_decimal(d["intrinsicValue"]),
        "last_trade": to_decimal(d["lastTrade"]),
        "low": to_decimal(d["low"]),
        "low52": to_decimal(d["low52"]),
        "market_cap": to_decimal(d["marketCap"]),
        "next_earning_date": d["nextEarningDate"],
        "open": to_decimal(d["open"]),
        "open_interest": d["openInterest"],
        "option_multiplier": to_decimal(d["optionMultiplier"]),
        "option_style": d["optionStyle"],
        "option_underlier": d["optionUnderlier"],
        "pe": to_decimal(d["pe"]),
        "previous_close": to_decimal(d["previousClose"]),
        "previous_day_volume": d["previousDayVolume"],
        "primary_exchange": d["primaryExchange"],
        "shares_outstanding": to_decimal(d["sharesOutstanding"]),
        "symbol_description": d["symbolDescription"],
        "time_premium": to_decimal(d["timePremium"]),
        "total_volume": to_decimal(d["totalVolume"]),
        "upc": d["upc"],
        "yield": to_decimal(d["yield"]),
    }
    if d.get("dividendPayableDate"):
        kwargs["dividend_payable_date"] = s_to_timestamp(d["dividendPayableDate"])
    if d.get("exDividendDate"):
        kwargs["ex_dividend_date"] = s_to_timestamp(d["exDividendDate"])
    if d.get("expirationDate"):
        kwargs["expiration_date"] = ms_to_timestamp(d["expirationDate"])
    if d.get("timeOfLastTrade"):
        kwargs["time_of_last_trade"] = s_to_timestamp(d["timeOfLastTrade"])
    if d.get("week52HiDate"):
        kwargs["week52_hi_date"] = s_to_timestamp(d["week52HiDate"])
    if d.get("week52LowDate"):
        kwargs["week52_low_date"] = s_to_timestamp(d["week52LowDate"])
    if "optionPreviousAskPrice" in d:
        kwargs["option_previous_ask_price"] = to_decimal(d["optionPreviousAskPrice"])
    if "optionPreviousBidPrice" in d:
        kwargs["option_previous_bid_price"] = to_decimal(d["optionPreviousBidPrice"])
    if "optionUnderlierExchange" in d:
        kwargs["option_underlier_exchange"] = d["optionUnderlierExchange"]
    if "osiKey" in d:
        kwargs["osi_key"] = d["osiKey"]
    return pb.QuoteAllDetail(**kwargs)


@_converts("quote")
def dict_to_quote(d: dict) -> pb.Quote:
    kwargs: dict[str, Any] = {
        "ah_flag": d["ahFlag"],
        "product": dict_to_product(d["Product"]),
        "quote_status": d["quoteStatus"],
    }
    if d.get("dateTimeUTC"):
        kwargs["date_time"] = s_to_timestamp(d["dateTimeUTC"])
    if "All" in d:
        kwargs["all"] = dict_to_quote_all_detail(d["All"])
    if "hasMiniOptions" in d:
        kwargs["has_mini_options"] = d["hasMiniOptions"]
    if "Option" in d:
        kwargs["option"] = dict_to_quote_option_detail(d["Option"])
    return pb.Quote(**kwargs)


def dict_to_instrument(d: dict) -> pb.Instrument:
    kwargs: dict[str, Any] = {
        "product": dict_to_product(d["Product"]),
        "order_action": d["orderAction"],
        "ordered_quantity": to_decimal(d["orderedQuantity"]),
        "quantity_type": d["quantityType"],
        "estimated_commission": to_decimal(d["estimatedCommission"]),
        "estimated_fees": to_decimal(d["estimatedFees"]),
        "filled_quantity": to_decimal(d["filledQuantity"]),
        "symbol_description": d["symbolDescription"],
    }
    if "averageExecutionPrice" in d:
        kwargs["average_execution_price"] = to_decimal(d["averageExecutionPrice"])
    return pb.Instrument(**kwargs)


def dict_to_order_event(d: dict) -> pb.OrderEvent:
    instruments = [dict_to_instrument(i) for i in d["Instrument"]]
    kwargs: dict[str, Any] = {
        "name": d["name"],
        "instruments": instruments,
    }
    if d.get("dateTime"):
        kwargs["date_time"] = ms_to_timestamp(d["dateTime"])
    return pb.OrderEvent(**kwargs)


@_converts("order")
def dict_to_order(d: dict) -> pb.Order:
    # The raw order dict has OrderDetail as a list with one element
    order_detail = d["OrderDetail"][0]

    events_data = order_detail.get("Events", {})
    event_list = events_data.get("Event", [])
    events = [dict_to_order_event(e) for e in event_list]

    kwargs: dict[str, Any] = {
        "order_id": str(d["orderId"]),
        "status": order_detail["status"],
        "events": events,
        "details_url": d.get("details", ""),
    }
    if order_detail.get("executedTime"):
        kwargs["executed_time"] = ms_to_timestamp(order_detail["executedTime"])
    if "orderType" in order_detail:
        kwargs["order_type"] = order_detail["orderType"]
    return pb.Order(**kwargs)


def dict_to_brokerage(d: dict) -> pb.Brokerage:
    kwargs: dict[str, Any] = {
        "transaction_type": d["transactionType"],
        "quantity": to_decimal(d["quantity"]),
        "price": to_decimal(d["price"]),
        "settlement_currency": d["settlementCurrency"],
        "payment_currency": d["paymentCurrency"],
        "fee": to_decimal(d["fee"]),
    }
    product_data = d.get("Product") or d.get("product")
    if product_data and product_data != {}:
        kwargs["product"] = dict_to_product(product_data)
    order_no = d.get("orderNo")
    if order_no and order_no != "" and int(order_no) != 0:
        kwargs["order_no"] = order_no
    return pb.Brokerage(**kwargs)


def dict_to_selected_ed(d: dict) -> pb.SelectedEd:
    return pb.SelectedEd(
        day=d["day"],
        month=d["month"],
        year=d["year"],
    )


def _dict_to_option_contract(d: dict, proto_cls: type) -> Any:
    kwargs: dict[str, Any] = {
        "adjusted_flag": d["adjustedFlag"],
        "ask": to_decimal(d["ask"]),
        "ask_size": d["askSize"],
        "bid": to_decimal(d["bid"]),
        "bid_size": d["bidSize"],
        "display_symbol": d["displaySymbol"],
        "in_the_money": d["inTheMoney"],
        "last_price": to_decimal(d["lastPrice"]),
        "net_change": to_decimal(d["netChange"]),
        "open_interest": d["openInterest"],
        "option_category": d["optionCategory"],
        "option_root_symbol": d["optionRootSymbol"],
        "option_type": d["optionType"],
        "osi_key": d["osiKey"],
        "quote_detail": d["quoteDetail"],
        "strike_price": to_decimal(d["strikePrice"]),
        "symbol": d["symbol"],
        "volume": d["volume"],
    }
    if d.get("timeStamp"):
        kwargs["time_stamp"] = s_to_timestamp(d["timeStamp"])
    if "OptionGreeks" in d:
        kwargs["option_greeks"] = dict_to_option_greeks(d["OptionGreeks"])
    return proto_cls(**kwargs)


def dict_to_option_pair(d: dict) -> pb.OptionPair:
    return pb.OptionPair(
        call=_dict_to_option_contract(d["Call"], pb.Call),
        put=_dict_to_option_contract(d["Put"], pb.Put),
    )


@_converts("option_chain_response")
def dict_to_option_chain_response(d: dict) -> pb.GetOptionChainsResponse:
    kwargs: dict[str, Any] = {
        "near_price": to_decimal(d["nearPrice"]),
        "option_pair": [dict_to_option_pair(p) for p in d["OptionPair"]],
        "quote_type": d["quoteType"],
        "selected_ed": dict_to_selected_ed(d["SelectedED"]),
    }
    if d.get("timeStamp"):
        kwargs["time_stamp"] = s_to_timestamp(d["timeStamp"])
    return pb.GetOptionChainsResponse(**kwargs)


def dict_to_expiration_date(d: dict) -> pb.ExpirationDate:
    return pb.ExpirationDate(
        day=d["day"],
        expiry_type=d["expiryType"],
        month=d["month"],
        year=d["year"],
    )


@_converts("option_expire_dates_response")
def dict_to_option_expire_dates_response(
    d: dict,
) -> pb.GetOptionExpireDatesResponse:
    return pb.GetOptionExpireDatesResponse(
        dates=[dict_to_expiration_date(ed) for ed in d["ExpirationDate"]],
    )


@_converts("transaction")
def dict_to_transaction(d: dict) -> pb.Transaction:
    brokerage_data: dict = d.get("Brokerage") or d.get("brokerage") or {}
    if not brokerage_data:
        raise ValueError("Transaction is missing brokerage data")
    kwargs: dict[str, Any] = {
        "transaction_id": str(d["transactionId"]),
        "account_id": d["accountId"],
        "amount": to_decimal(d["amount"]),
        "description": d["description"],
        "brokerage": dict_to_brokerage(brokerage_data),
    }
    if d.get("transactionDate"):
        kwargs["transaction_date"] = ms_to_timestamp(d["transactionDate"])
    if d.get("postDate"):
        kwargs["post_date"] = ms_to_timestamp(d["postDate"])
    return pb.Transaction(**kwargs)
