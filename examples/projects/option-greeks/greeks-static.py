# Option Greeks with Databento and Pathway
# The static case: the data is read from CSV files
import math
import os

import pandas as pd
import scipy
import streamlit_ux
from dotenv import load_dotenv
from scipy.stats import norm

import pathway as pw

# The start time is used to compute the time to expiration
start_time = pd.Timestamp("2024-04-04T17:00:00", tz="Us/Central")
_data_duration = pd.Timedelta(hours=1)
interest_rate = 0.043

front_month_symbol = "ESM4"

# The definition data contains 1 day of data, starting at start_time
definitions_path = "./data/definition.csv"
# The order book data contains 2 minutes of `mbp-1` data starting with start_time
options_path = "./data/options.csv"


class DefinitionInputSchema(pw.Schema):
    ts_recv: int  # Time in ns when the data was received
    raw_symbol: str  # symbol of option
    expiration: int  # expiration time of the option
    instrument_class: str  # type of option
    strike_price: float  # see below for what it means
    underlying: str  # symbol of the first underlying instrument
    instrument_id: int  # An identifier of the option


table_es = pw.io.csv.read(definitions_path, schema=DefinitionInputSchema, mode="static")

# Only keep definitions for asset with the correct underlying instrument
table_esm4 = table_es.filter(pw.this.underlying == front_month_symbol)

# Filter out instruments that are not **CALL** nor **PUT** options
table_esm4 = table_esm4.filter(
    (pw.this.instrument_class == "C") | (pw.this.instrument_class == "P")
)


class OptionInputSchema(pw.Schema):
    raw_symbol: str  # the symbol of the option
    bid_px: float  # the bid price of that option
    ask_px: float  # the ask price of that option


table_mbp1 = pw.io.csv.read(options_path, schema=OptionInputSchema, mode="static")

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


table_texp = table_prices.with_columns(
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


# The volatility is computed by solving the following equation, by finding the roots of
#
# $$
# BlackPrice(\sigma) - Meanprice = 0
# $$
#
# using `scipy`'s root finding function.
# To mark the non-convergence/non-existence of a root, `None` is returned.
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

load_dotenv()
port = int(os.environ.get("PORT"))

streamlit_ux.send_table_to_web(port, table_greeks, "table_greeks")
pw.run()
