# Option Greeks with Databento and Pathway
# The replay case: the data is read from Databento Historical API.
import math
import os
import time

import databento as db
import pandas as pd
import scipy
import streamlit_ux
from dotenv import load_dotenv
from scipy.stats import norm

import pathway as pw

load_dotenv()
API_KEY = os.environ.get("API_KEY")
client = db.Historical(API_KEY)

time_between_updates = 0.05  # seconds

db_dataset = "GLBX.MDP3"  # CME Globex MDP 3.0
db_def_schema = "definition"
db_price_schema = "mbp-1"
db_def_symbols = ["ES.OPT"]  # to refer to all options whose root symbol is ES

front_month_symbol = "ESM4"
interest_rate = 0.043

# For historical data, the start time and duration are also needed:
start_time = pd.Timestamp("2024-04-04T17:00:00", tz="Us/Central")
data_duration = pd.Timedelta(days=1)
# Only 2 minutes
query_data_duration = pd.Timedelta(minutes=2)


class DefinitionInputSchema(pw.Schema):
    ts_recv: int  # Time in ns when the data was received
    raw_symbol: str  # symbol of option
    expiration: int  # expiration time of the option
    instrument_class: str  # type of option
    strike_price: float  # the price at which one can sell/buy the option
    underlying: str  # symbol of the first underlying instrument
    instrument_id: int  # An identifier of the option


class DefinitionSubject(pw.io.python.ConnectorSubject):
    def run(self):
        data = client.timeseries.get_range(
            dataset=db_dataset,
            schema=db_def_schema,
            symbols=db_def_symbols,
            stype_in=db.SType.PARENT,
            start=start_time,
            end=start_time + data_duration,
        )

        for row in data:
            # Get the attributes we are interested in
            ts_recv = getattr(row, "ts_recv")
            raw_symbol = getattr(row, "raw_symbol")
            expiration = getattr(row, "expiration")
            instrument_class = getattr(row, "instrument_class")
            strike_price = getattr(row, "strike_price") / 1e9
            underlying = getattr(row, "underlying")
            instrument_id = getattr(row, "instrument_id")

            # Transmit the data in json format
            self.next(
                ts_recv=ts_recv,
                raw_symbol=raw_symbol,
                expiration=expiration,
                instrument_class=instrument_class,
                strike_price=strike_price,
                underlying=underlying,
                instrument_id=instrument_id,
            )
            # No time.sleep() as this data is static


table_es = pw.io.python.read(DefinitionSubject(), schema=DefinitionInputSchema)

# Only keep definitions for asset with the correct underlying instrument
table_esm4 = table_es.filter(pw.this.underlying == front_month_symbol)

# Filter out instruments that are not **CALL** nor **PUT** options
table_esm4 = table_esm4.filter(
    (pw.this.instrument_class == "C") | (pw.this.instrument_class == "P")
)

# Extracting all the relevant symbols to query the associated orders.
table_symbols = table_esm4.reduce(symbol_tuple=pw.reducers.tuple(pw.this.raw_symbol))
symbol_list = [front_month_symbol] + list(
    pw.debug.table_to_pandas(table_symbols)["symbol_tuple"][0]
)


class OptionInputSchema(pw.Schema):
    raw_symbol: str  # the symbol of the option
    bid_px: float  # the bid price of that option
    ask_px: float  # the ask price of that option


class MBP1Subject(pw.io.python.ConnectorSubject):
    def run(self):
        data = client.timeseries.get_range(
            dataset=db_dataset,
            schema=db_price_schema,
            start=start_time,
            end=start_time + query_data_duration,
            symbols=symbol_list,
        )
        # Datebento's instrument map, which will help us get the symbols from the row data
        instrument_map = db.common.symbology.InstrumentMap()
        instrument_map.insert_metadata(data.metadata)
        for row in data:
            symbol = instrument_map.resolve(
                row.instrument_id, row.pretty_ts_recv.date()
            )
            levels = getattr(row, "levels")
            # INT64_MAX is the mark for unknown bid/ask prices
            if levels[0].bid_px > (1 << 63) - 10 or levels[0].ask_px > (1 << 63) - 10:
                continue
            # Prices unit is actually 1e-9
            bid_px = levels[0].bid_px / 1e9
            ask_px = levels[0].ask_px / 1e9
            self.next(
                raw_symbol=symbol,
                bid_px=bid_px,
                ask_px=ask_px,
            )

            """
            This is the only difference between static and replay.
            """
            time.sleep(time_between_updates)


table_mbp1 = pw.io.python.read(MBP1Subject(), schema=OptionInputSchema)

# Compute the average prices for all bids using a groupby/reduce
table_mbp1 = table_mbp1.groupby(pw.this.raw_symbol).reduce(
    raw_symbol=pw.this.raw_symbol,
    option_midprice=(pw.reducers.avg(pw.this.bid_px) + pw.reducers.avg(pw.this.ask_px))
    / 2,
)

# Add the data from the `table_definitions` to `table_options` using a join.
table_prices = table_esm4.join(
    table_mbp1, pw.left.raw_symbol == pw.right.raw_symbol
).select(
    *pw.left,  # Adding all the columns from table_definitions
    option_midprice=pw.right.option_midprice,
)

table_prices = table_prices.with_columns(
    future_price=table_mbp1.ix_ref(front_month_symbol).option_midprice
)


# Compute the time to expiration, has to be in years
@pw.udf
def compute_time_to_expiration(expiration_time: int) -> float:
    return (expiration_time - int(start_time.timestamp() * 1e9)) / (1e9 * 86400 * 365)


table_texp = table_prices + table_prices.select(
    time_to_expiration=compute_time_to_expiration(pw.this.expiration)
)


# Compute the price using in the Black Model
def compute_price(
    F: float,
    K: float,
    T: float,
    sigma: float,
    r: float = interest_rate,
    is_call: bool = True,
) -> float:
    d1 = (math.log(F / K) + (sigma**2 / 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    sign = 2 * int(is_call) - 1

    return math.exp(-r * T) * sign * (norm.cdf(sign * d1) * F - norm.cdf(sign * d2) * K)


# The volatility is computed by solvin the following equation, by finding the roots of
#
# $$
# BlackPrice(\sigma) - Meanprice = 0
# $$
#
# using `scipy`'s root finding function.
# To mark the non-convergence/non-existence of a root, None is returned.
@pw.udf
def compute_volatility(
    F: float, K: float, T: float, is_call: bool, option_midprice: float
) -> float | None:
    result = scipy.optimize.root_scalar(
        lambda sigma: option_midprice
        - compute_price(F=F, K=K, T=T, sigma=sigma, is_call=is_call),
        x0=0.0001,
        x1=0.8,
    )

    return result.root if result.converged else None


table_texp = table_texp.with_columns(is_call=pw.this.instrument_class == "C")

# We now simply add the volatility column, using the `select` operation on a Table.
table_volatility_unfiltered = table_texp.with_columns(
    volatility=compute_volatility(
        pw.this.future_price,
        pw.this.strike_price,
        pw.this.time_to_expiration,
        pw.this.is_call,
        pw.this.option_midprice,
    )
)

# Filter invalid entries, where volatility couldn't be computed
table_sigma = table_volatility_unfiltered.filter(pw.this.volatility.is_not_none())


# We will now compute the $d_1$, $d_2$ defined as before.
@pw.udf
def get_d1(
    F: float,
    K: float,
    T: float,
    sigma: float,
    r: float = interest_rate,
) -> float:
    return (math.log(F / K) + (sigma**2 / 2) * T) / (sigma * math.sqrt(T))


@pw.udf
def get_d2(
    F: float, K: float, T: float, sigma: float, r: float = interest_rate
) -> float:
    return (math.log(F / K) + (sigma**2 / 2) * T) / (
        sigma * math.sqrt(T)
    ) - sigma * math.sqrt(T)


# And now compute the greeks
table_d1d2 = table_sigma.with_columns(
    d1=get_d1(
        pw.this.future_price,
        pw.this.strike_price,
        pw.this.time_to_expiration,
        pw.this.volatility,
    ),
    d2=get_d2(
        pw.this.future_price,
        pw.this.strike_price,
        pw.this.time_to_expiration,
        pw.this.volatility,
    ),
)


@pw.udf
def compute_delta(
    F: float,
    K: float,
    T: float,
    sigma: float,
    d1: float,
    d2: float,
    is_call: bool,
    r: float = interest_rate,
) -> float:
    return (
        math.exp(-r * T) * norm.cdf(d1)
        if is_call
        else -math.exp(-r * T) * norm.cdf(-d1)
    )


@pw.udf
def compute_gamma(
    F: float,
    K: float,
    T: float,
    sigma: float,
    d1: float,
    d2: float,
    is_call: bool,
    r: float = interest_rate,
) -> float:
    return math.exp(-r * T) * norm.pdf(d1) / (F * sigma * math.sqrt(T))


@pw.udf
def compute_theta(
    F: float,
    K: float,
    T: float,
    sigma: float,
    d1: float,
    d2: float,
    is_call: bool,
    r: float = interest_rate,
) -> float:
    term = -F * sigma * norm.pdf(d1) / (2 * math.sqrt(T))
    return (
        (
            term
            - r * K * math.exp(-r * T) * norm.cdf(d2)
            + r * F * math.exp(-r * T) * norm.cdf(d1)
        )
        / 252
        if is_call
        else (
            term
            + r * K * math.exp(-r * T) * norm.cdf(-d2)
            - r * F * math.exp(-r * T) * norm.cdf(-d1)
        )
        / 252
    )


@pw.udf
def compute_vega(
    F: float,
    K: float,
    T: float,
    sigma: float,
    d1: float,
    d2: float,
    is_call: bool,
    r: float = interest_rate,
) -> float:
    return F * norm.pdf(d1) * math.sqrt(T) * math.exp(-r * T) / 100


@pw.udf
def compute_rho(
    F: float,
    K: float,
    T: float,
    sigma: float,
    d1: float,
    d2: float,
    is_call: bool,
    r: float = interest_rate,
) -> float:
    return (
        -T * math.exp(-r * T) * (F * norm.cdf(d1) - K * norm.cdf(d2)) / 100
        if is_call
        else -T * math.exp(-r * T) * (K * norm.cdf(-d2) - F * norm.cdf(-d1)) / 100
    )


table_greeks = table_d1d2.select(
    ts_recv=pw.this.ts_recv,
    instrument_id=pw.this.instrument_id,  # option identifier
    delta=compute_delta(
        F=pw.this.future_price,
        K=pw.this.strike_price,
        T=pw.this.time_to_expiration,
        sigma=pw.this.volatility,
        d1=pw.this.d1,
        d2=pw.this.d2,
        is_call=pw.this.is_call,
    ),
    gamma=compute_gamma(
        F=pw.this.future_price,
        K=pw.this.strike_price,
        T=pw.this.time_to_expiration,
        sigma=pw.this.volatility,
        d1=pw.this.d1,
        d2=pw.this.d2,
        is_call=pw.this.is_call,
    ),
    theta=compute_theta(
        F=pw.this.future_price,
        K=pw.this.strike_price,
        T=pw.this.time_to_expiration,
        sigma=pw.this.volatility,
        d1=pw.this.d1,
        d2=pw.this.d2,
        is_call=pw.this.is_call,
    ),
    vega=compute_vega(
        F=pw.this.future_price,
        K=pw.this.strike_price,
        T=pw.this.time_to_expiration,
        sigma=pw.this.volatility,
        d1=pw.this.d1,
        d2=pw.this.d2,
        is_call=pw.this.is_call,
    ),
    rho=compute_rho(
        F=pw.this.future_price,
        K=pw.this.strike_price,
        T=pw.this.time_to_expiration,
        sigma=pw.this.volatility,
        d1=pw.this.d1,
        d2=pw.this.d2,
        is_call=pw.this.is_call,
    ),
)

port = int(os.environ.get("PORT"))

streamlit_ux.send_table_to_web(port, table_greeks, "table_greeks")
pw.run()
