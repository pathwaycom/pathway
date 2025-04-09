# ---
# title: "Computing the Option Greeks using Pathway and Databento"
# description: "Computing the Option Greeks using Pathway and Databento, in the Black Model"
# author: 'luca'
# article:
#   thumbnail: '/assets/content/showcases/option-greeks/option-greeks.png'
#   thumbnailFit: 'contain'
#   date: '2024-08-06'
#   tags: ['tutorial', 'data-pipeline']
# keywords: ['Option Greeks', 'Databento', 'Delta', 'Gamma', 'Theta', 'Rho', 'quant', 'Black model', 'notebook']
# notebook_export_path: notebooks/tutorials/option-greeks.ipynb
# docker_github_link: "https://github.com/pathwaycom/pathway/tree/main/examples/projects/option-greeks"
# aside: true
# ---

# # Option Greeks with Databento and Pathway
#
# ## Introduction
#
# Option Greeks are essential tools in financial risk management, measuring an option's price sensitivity. In this article, you'll learn to compute Option Greeks using the [Black/Black 76 model](https://en.wikipedia.org/wiki/Black_model), a variant of the more known [Black-Scholes model](https://en.wikipedia.org/wiki/Black%E2%80%93Scholes_model), and Databento's real-time and historical market data APIs. You'll compute the key Option Greeks, Delta, Gamma, Theta, Vega, and Rho, updating these values in real-time with Pathway to match the real-time data provided by Databento.
#
# You can find the sources of the entire project in our [public repository](https://github.com/pathwaycom/pathway/tree/main/examples/projects/option-greeks).
#
# ### About Pathway
#
# Pathway is a Python data processing framework for analytics and AI pipelines over data streams. It’s the ideal solution for real-time processing use cases like computing the Option Greeks.
# Pathway comes with an easy-to-use Python API, syntax that is simple and intuitive, and you can use the same code for both batch and streaming processing.
# Pathway is powered by a scalable Rust engine based on Differential Dataflow and performing incremental computation.
#
# ### About Databento
#
# [Databento](https://databento.com/) is a market data provider aiming at making access to institutional-grade financial data simpler and faster.
# By providing the data directly without third-parties, Databento provides market data APIs with low latency and without data loss.
# Their simple Python APIs are an ideal data source to perform real-time financial analysis using Pathway.
# Don't hesitate to browse [Databento catalog](https://databento.com/venues) to see all the available data.
#
# ![Option Greeks workflow with Pathway and Databento](/assets/content/showcases/option-greeks/option-greeks.svg)
#
# ### Option Greeks
#
# Options are financial derivatives that give the holder the right, but not the obligation, to buy or sell an underlying asset at a specified price within a certain period.
# On the other hand, **futures** are contracts that obligate the parties involved to buy or sell an asset at a predetermined price on a specified future date.
# Options are commonly used in various financial strategies, including hedging and speculation.
#
# In this tutorial, you are going to manipulate **options on futures**.
#
# There are two kind of options: (1) **call options** and (2) **put options**.
# A call option gives the holder the right, but not the obligation, to buy the underlying asset at a specified price (the strike price) before or at the option's expiration date.
# On the other hand, a put option gives the holder the right, but not the obligation, to sell the underlying asset at the strike price before or at the expiration date.
#
# In options trading, **Option Greeks** are metrics used to assess the sensitivity of option prices and provide detailed, quantifiable measures of various risk factors.
# Using Option Greeks, traders and risk managers make more informed, strategic decisions, enhancing their ability to manage risk and optimize returns.
#
# Each Option Greek measures the sensisibility of option prices to different factors:
# - Delta ($\Delta$) measures the change in an option's price relative to the change in the underlying asset's price. For a call option, Delta is positive, ranging from 0 to 1. This is because as the underlying asset's price increases, the call option becomes more valuable. For a put option, Delta is negative, ranging from -1 to 0. This is because as the underlying asset's price decreases, the put option becomes more valuable.
# - Gamma ($\Gamma$) indicates the rate of change of Delta with respect to changes in the price of the underlying asset for both calls and puts. A high Gamma indicates that Delta can change rapidly. Monitoring Gamma helps in understanding how stable an option's Delta is.
# - Theta ($\Theta$) represents the time decay of an option. For call options, Theta is usually negative, indicating the option loses value as time passes, given all else remains constant. For put options, Theta is also negative, but the rate at which the value decays can differ because the time decay effect can be more pronounced in different market conditions and volatilities.
# - Vega ($V$) measures sensitivity to volatility for both calls and puts. Higher volatility increases the value of both calls and puts, but the degree can vary depending on whether it's a call or a put and their respective positions relative to the strike price.
# - Rho ($\rho$) indicates sensitivity to interest rate changes. For call options, Rho is positive, meaning the value of a call option increases as interest rates rise. For put options, Rho is negative, meaning the value of a put option decreases as interest rates rise.
#
# These metrics help traders and risk managers understand and hedge the risks associated with options positions.
# You can learn more about Option Greeks [here](https://en.wikipedia.org/wiki/en:Greeks_(finance)) .
#
# #### Generic formulas
#
# First, let's introduce some notations.
# Let's define $d_1, d_2$ to help us define the Greeks.
# Here,
# - $F$ is the futures price, the agreed-upon price in a futures contract between 2 parties, of an asset, at a specified time in the future
# - $K$ is the strike price, the price at which one can sell/buy that option,
# - $T$ is the time to expiration in years,
# - $r$ is the risk-free interest rate, the theoretical rate of return of an investment with zero risk,
# - $\sigma$ is the volatility, the variation of prices over some time interval, also known as the standard deviation of logarithmic returns,
# - $N(x)$ is the standard normal cumulative distribution function,
# - $N'(x)$ is the standard normal probability density function.
#
# $$
# d_1 = \frac{ln(\frac{F}{K}) + (0.5 \cdot \sigma^2)T}{\sigma \sqrt{T}}, d_2 = d_1 - \sigma\sqrt{T}
# $$
#
# The Option Greeks are defined using $d_1$ and $d_2$.
# Formula changes depending on whether the option is a call or a put.
#
# #### Delta ($\Delta$)
# - Call option: $\Delta = e^{-r T} N(d_1)$
# - Put option: $\Delta = -e^{-r T}N(-d_1)$
#
# #### Gamma ($\Gamma$)
# - For both call and put options: $\Gamma = e^{-r T}\frac{N'(d_1)}{S \sigma \sqrt{T}}$
#
# #### Theta ($\Theta$)
# - For call option:
# $$
# \Theta = \frac{\frac{-F\sigma N'(d_1)}{2\sqrt{T}}-r \cdot K \cdot e^{-r T} N(d_2)) + r \cdot F \cdot e^{-r T}N(d_1)}{252}
# $$
# - For put option
# $$
# \Theta = \frac{\frac{-F\sigma N'(d_1)}{2\sqrt{T}}+r \cdot K \cdot e^{-r T} N(-d_2))- r \cdot F \cdot e^{-r T}N(-d_1)}{252}
# $$
#
# #### Vega ($V$)
# - For both call and put options: $V = e^{-r T}\frac{F \cdot N'(d_1) \sqrt{T}}{100}$
#
# #### Rho ($\rho$)
# - For call option:
# $$
# \rho = -T e^{-r T} \frac{F \cdot N(d_1) - K \cdot N(d_2)}{100}
# $$
# - For put option:
# $$
# \rho = -T e^{-r T} \frac{K \cdot N(-d_2) - F \cdot N(-d_1)}{100}
# $$
#
# Our article is inspired by [this article from Databento](https://databento.com/docs/examples/options/estimating-implied-volatility), which computes the _volatility_ for options for the front-month contract. You can check out their page for more insight.
#
# Let's get started!

# ## Getting started
#
# ### Get your API key
#
# First, you need financial data. Stock market data is usually public and can be accessed using APIs. [Databento](https://databento.com/docs) provides simple and fast Python APIs to access market data. You can [signup](https://databento.com/signup) and get free credits. You will obtain an API key with your account: save it, you will need it to access the data.
#
# ### Install the required packages
#
# To continue, make sure to install all the needed packages.
#
# ```
# # !pip install pathway databento pandas scipy numpy python-dotenv
# ```

# Let's start by importing Pathway and Databento:

#_MD_SHOW_import databento as db
import pathway as pw

# ### Load the API keys using an `.env` file
#
# You need to use the API key to access Databento's data. You can create an `.env` file and set an environment variable or paste the API key directly into the code.
#
# To use the `.env` file, you first need to create the file, and then copy the key directly in the file:
# ```
# API_KEY = "********"
# ```
#
# Then, you need to use the `os` package to load the variable:

# +
import os
from dotenv import load_dotenv


load_dotenv()
API_KEY = os.environ.get("API_KEY")
#_MD_SHOW_client = db.Historical(API_KEY)
# -

# ## Getting the data
#
# Let's start with static data and switch to streaming once everything is ready. Both Databento and Pathway make switching to streaming very easy.
#
# ### Data
# Let's focus on E-mini S&P 500 futures contracts whose associated symbol is `ES`.
# The data is from the Globex exchange platform of the CME Group.
#
# #### Notation
# Options are represented by a specific formatting: root symbol + month + year then type + strike price.
# Let's take `ESM4 C2950` as an example:
# - Its root symbol is `ES`, it is a E-mini S&P 500 future contract.
# - M4 refers to the expiration month and year of the contract: it expires in June 2024.
# - C means that it is a call option. P is used for puts.
# - 2950 is the strike price of the option. It is the prices at which the holder can buy the ESM4 future contract.
#
# `ESM4` is called the **underlying asset of the of the option**, it is the E-mini S&P 500 futures contract that expires in June 2024.

# ### Definitions and orders
#
# The data is separated into two datasets: the definitions and the orders.
#
# **Definitions** provides reference information about each instrument. For options, you are going to use those:
# - The symbol of option.
# - The identifier of the option.
# - The underlying future.
# - The type of option.
# - The expiration time of the option.
# - The strike price of the option.
# - The time of reception of the data.
#
# **This data is static by nature and do not change over time.**
#
# In addition to the definitions, you also need the state of the market.
# You will use the **order book**. For options, you will need:
# - The symbol of the option.
# - The bid price.
# - The ask price.
#
# You won't need the times of the order as only the orders from the requested time slot will be received.
# **The book order data is dynamic by nature as new orders arrive over time.**
# However, this article will focus on querying historical data: past orders from a given time period.
# This data is static as it is data from a given time period: all the orders of this time period are known and included in the data.
#
#
# ### Reading the definitions
# The CME Globex data can be found in the [**GLBX.MDP3** dataset](https://databento.com/datasets/GLBX.MDP3) and the [**definition** schema](https://databento.com/docs/schemas-and-data-formats/instrument-definitions?historical=python&live=python). This provides us with useful information about our options, such as the *option type* or *expiration time* of the future.

db_dataset = "GLBX.MDP3" # CME Globex MDP 3.0
db_def_schema = "definition" # this provides us the reference information of each instrument
db_def_symbols = ["ES.OPT"] # all options whose root symbol is ES

# For historical data, the start time and duration are also needed:

# +
import pandas as pd

start_time = pd.Timestamp("2024-04-04T17:00:00", tz="Us/Central")
data_duration = pd.Timedelta(days=1)


# -

# Let's define a custom [Pathway connector](/developers/user-guide/connect/connectors/custom-python-connectors), to read the data directly from Databento's API.
# First, you need to declare the [schema](/developers/user-guide/connect/schema) of the data:

class DefinitionInputSchema(pw.Schema):
    ts_recv: int          # time in ns when the data was received
    raw_symbol: str       # symbol of option
    expiration: int       # expiration time of the option
    instrument_class: str # type of option
    strike_price: float   # see below for what it means
    underlying: str       # symbol of the first underlying instrument
    instrument_id: int    # An identifier of the option


# Now, you need to define a `ConnectorSubject` that will define how the data is read and ingested by Pathway.
# The market data is accessed in the `ConnectorSubject` using Databento's [time series](https://databento.com/docs/api-reference-historical/timeseries?historical=python&live=python):

class DefinitionSubject(pw.io.python.ConnectorSubject):
    def run(self):
        # First, get Databento's data:
# _MD_SHOW_        data = client.timeseries.get_range(
# _MD_SHOW_            dataset=db_dataset,
# _MD_SHOW_            schema=db_def_schema,
# _MD_SHOW_            symbols=db_def_symbols,
# _MD_SHOW_            stype_in=db.SType.PARENT,
# _MD_SHOW_            start=start_time,
# _MD_SHOW_            end=start_time + data_duration,
# _MD_SHOW_        )
# _MD_COMMENT_START_
        data = []
# _MD_COMMENT_END_
        # Now, ingest the data into Pathway:
        for row in data:
            # Get the attributes you are interested in
            ts_recv = getattr(row, "ts_recv")
            raw_symbol = getattr(row, "raw_symbol")
            expiration = getattr(row, "expiration")
            instrument_class = getattr(row, "instrument_class")
            strike_price = getattr(row, "strike_price") / 1e9
            underlying = getattr(row, "underlying")
            instrument_id = getattr(row, "instrument_id")

            # Transmit the data
            self.next(
                ts_recv=ts_recv,
                raw_symbol=raw_symbol,
                expiration=expiration,
                instrument_class=instrument_class,
                strike_price=strike_price,
                underlying=underlying,
                instrument_id=instrument_id,
            )


# You can read more about how to access Databento data in [Databento's Documentation](https://databento.com/docs/).
#
# Now you can create the table using the [Python connector](/developers/user-guide/connect/connectors/custom-python-connectors):

# _MD_SHOW_table_es = pw.io.python.read(DefinitionSubject(), schema=DefinitionInputSchema)
# _MD_COMMENT_START_
table_es = pw.io.csv.read('definition.csv', schema=DefinitionInputSchema, mode='static')
pw.debug.compute_and_print(table_es, n_rows=5)
# _MD_COMMENT_END_


# ### Extracting relevant options data

# Currently, your table contain all the options.
# You need to keep only the one you are interested in.
# Among all the futures corresponding to the associated future `ES`, let's focus on `ESM4`.
# In practice you may be interested in the front-month symbol, the one that has the closest expiration date.

front_month_symbol = 'ESM4'

# Let's filter the data to only keep the options associated with `ESM4` futures.
# To only compute the Greeks for the correct options, let's filter out all those whose underlying instrument is different from `ESM4`:

table_esm4 = table_es.filter(pw.this.underlying==front_month_symbol)
# _MD_COMMENT_START_
pw.debug.compute_and_print(table_esm4, n_rows=5)
# _MD_COMMENT_END_

# As a safeguard, let's also filter on the `instrument_class` to make sure the value is either `C` (CALL) or `P` (PUT):

table_esm4 = table_esm4.filter((pw.this.instrument_class == 'C') | (pw.this.instrument_class == 'P'))
# _MD_COMMENT_START_
pw.debug.compute_and_print(table_esm4, n_rows=5)
# _MD_COMMENT_END_

# ### Option orders
#
# Now that you have the options, you need to find the associated orders.
# The prices, used to compute the volatility, are obtained by averaging all the bids and ask prices. Hence, mid-price is the correct term.
#
# This data will be obtained from the _mbp-1_ schema ([Market by price](https://databento.com/docs/schemas-and-data-formats/mbp-1?historical=python&live=python)), which provides every event that updates the top price.
#
# To limit data usage, you can query only the symbols present in your data:

table_symbols = table_esm4.reduce(symbol_tuple = pw.reducers.tuple(pw.this.raw_symbol))
symbol_list = [front_month_symbol] + list(pw.debug.table_to_pandas(table_symbols)['symbol_tuple'][0])

# As an example, let's only query over a _2 minutes_ time interval, to limit the number of data to extract and process.
# Similarly to the definition schema, you will use a custom connector to read the data from Databento:

# +
db_price_schema = "mbp-1"

class MBP1InputSchema(pw.Schema):
    raw_symbol: str # the symbol of the option
    bid_px: float   # the bid price
    ask_px: float   # the ask price

# Only 2 minutes
query_data_duration = pd.Timedelta(minutes=2)

class MBP1Subject(pw.io.python.ConnectorSubject):
    def run(self):
# _MD_SHOW_        data = client.timeseries.get_range(
# _MD_SHOW_            dataset=db_dataset,
# _MD_SHOW_            schema=db_price_schema,
# _MD_SHOW_            start=start_time,
# _MD_SHOW_            end=start_time + query_data_duration,
# _MD_SHOW_            symbols=symbol_list
# _MD_SHOW_        )
        # Databento's instrument map, which will help us get the symbols from the row data
        # _MD_SHOW_instrument_map = db.common.symbology.InstrumentMap()
        # _MD_SHOW_instrument_map.insert_metadata(data.metadata)
# _MD_COMMENT_START_
        data = []
# _MD_COMMENT_END_
        for row in data:
            symbol = instrument_map.resolve(row.instrument_id, row.pretty_ts_recv.date())
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

# _MD_SHOW_table_mbp1 = pw.io.python.read(MBP1Subject(), schema=MBP1InputSchema)
# _MD_COMMENT_START_
table_mbp1 = pw.io.csv.read('options.csv', schema=MBP1InputSchema, mode='static')
pw.debug.compute_and_print(table_mbp1, n_rows=5)
# _MD_COMMENT_END_
# -

# To compute the average prices for all bids, you can use a simple [groupby/reduce](https://pathway.com/developers/user-guide/data-transformation/groupby-reduce-manual):

table_mbp1 = table_mbp1.groupby(pw.this.raw_symbol).reduce(
    raw_symbol=pw.this.raw_symbol,
    option_midprice=(pw.reducers.avg(pw.this.bid_px) + pw.reducers.avg(pw.this.ask_px)) / 2,
)
# _MD_COMMENT_START_
pw.debug.compute_and_print(table_mbp1, n_rows=5)
# _MD_COMMENT_END_

# Now, you want to add the data from the `table_esm4` to this `table_mbp1`.
# To do so, you need to join the two table on the `raw_symbol` values:

# +
table_prices = table_esm4.join(
    table_mbp1,
    pw.left.raw_symbol == pw.right.raw_symbol
).select(
    *pw.left,  # Adding all the columns from table_esm4
    option_midprice=pw.right.option_midprice,
)

# _MD_COMMENT_START_
pw.debug.compute_and_print(table_prices, n_rows=5)
# _MD_COMMENT_END_
# -

# ## Computing the Option Greeks
#
# Now that the data is ready, you can compute the Option Greeks.
# To do so, you need several values, as you have seen in the **Generic Formulas** section. Recall that you need:
# - $F$, the futures price
# - $K$, the strike price
# - $T$, the time to expiration
# - $r$, the risk-free interest rate
# - $\sigma$, the volatility.
#
# ### Risk-free interest rate
#
# This is a bit more complicated, as it is usually determined by the market and is taken from government securities considered free of default risk. This can be considered more of an input parameter given by the user.
#
# Let's use $0.043$ as the value for this interest rate, based on government available data.

interest_rate = 0.043

# ### Futures price
#
# In the **[Black/Black 76 Model](https://en.wikipedia.org/wiki/Black_model)**, the Option Greeks are the options of the front-month contract, this value will be fixed per each front-month contract.
# In this case, it will be the _stock price_/_last price_ of the `ESM4` option. You can access rows by value using [`ix_ref`](https://pathway.com/developers/user-guide/data-transformation/indexing-grouped-tables):

# +
table_prices = table_prices.with_columns(
    future_price=table_mbp1.ix_ref(front_month_symbol).option_midprice
)

# _MD_COMMENT_START_
pw.debug.compute_and_print(table_prices, n_rows=5)
# _MD_COMMENT_END_
# -

# ### Time to expiration
#
# Using the `expiration` column, you can compute the _time to expiration_ $T$ which is used in our formulas. Because of how the formulas are defined, it is expressed in **years**.
#
# Be careful, the unit of `expiration` is nanoseconds.

# +
# Compute the time to expiration, has to be in years
@pw.udf
def compute_time_to_expiration(expiration_time: int) -> float:
    return (expiration_time - int(start_time.timestamp() * 1e9)) / (1e9 * 86400 * 365)


table_texp = table_prices.with_columns(
    time_to_expiration=compute_time_to_expiration(pw.this.expiration)
)

# _MD_COMMENT_START_
pw.debug.compute_and_print(table_texp, n_rows=5)
# _MD_COMMENT_END_
# -

# ### Implied Volatility
#
# The volatility represents the market's expectation of the future volatility of the underlying asset over the life of the option.
# Unlike historical volatility, which measures past price fluctuations, implied volatility is derived from the market price of the option itself.
#
# Using the [Black Model](https://en.wikipedia.org/wiki/Black_model), you can infer volatility using known data.
# The option price will be estimated as being **the average of the prices associated to that _option symbol_**.
# You need to compute $\sigma$ (the volatility) so that the price calculated in the Black Model is the same as the estimated option price.
# It comes down to finding the root of a polynomial, which will be done using **scipy**.

# ### Computing the volatility
#
# What follows next is just the previous formulas being translated into Python.
#
# First, you need to define the function to compute the option price in the Black Model, having computed the volatility, $\sigma$.

# +
import math

def compute_price(
    F: float,
    K: float,
    T: float,
    sigma: float,
    r: float=interest_rate,
    is_call: bool=True
) -> float:
    d1 = (math.log(F / K) + (sigma**2 / 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    sign = 2 * int(is_call) - 1

    return math.exp(-r * T) * sign * (norm.cdf(sign * d1) * F - norm.cdf(sign * d2) * K)


# -

# Now, solve the equation by finding the roots of
#
# $$
# BlackPrice(\sigma) - Meanprice = 0
# $$
#
# using `scipy`'s root finding function. To mark the non-convergence/non-existence of a root, return $None$.

# +
import scipy
from scipy.stats import norm

@pw.udf
def compute_volatility(
    F: float,
    K: float,
    T: float,
    _is_call: bool,
    option_midprice: float
) -> float | None:
    result = scipy.optimize.root_scalar(
        lambda sigma: option_midprice - compute_price(
            F=F,
            K=K,
            T=T,
            sigma=sigma,
            is_call=_is_call
        ),
        x0=0.0001,x1=0.8
    )

    return result.root if result.converged else None


# -

# ### Computing $d_1$, $d_2$
#
# Now, let's compute the $d_1$, $d_2$ defined as before. Let's define those functions as [**Pathway user-defined function**](/developers/user-guide/data-transformation/user-defined-functions) using the `pw.udf` decorator. Another alternative would be to declare the functions as simple Python functions and apply them to the columns using [`pw.apply`](/developers/user-guide/data-transformation/table-operations#column-operations).

# +
@pw.udf
def compute_d1(
    F: float,
    K: float,
    T: float,
    sigma: float,
    r: float = interest_rate,
) -> float:
    return (math.log(F / K) + (sigma**2 / 2) * T) / (sigma * math.sqrt(T))

@pw.udf
def compute_d2(
    F: float,
    K: float,
    T: float,
    sigma: float,
    r: float = interest_rate
) -> float:
    return (math.log(F / K) + (sigma**2 / 2) * T) / (sigma * math.sqrt(T)) - sigma * math.sqrt(T)


# -

# ### Option Greeks functions

# You can now define the functions to compute the Option Greeks using $F$, $K$, $T$, $\sigma$, $d_1$, $d_2$, and $\rho$:

# +
@pw.udf
def compute_delta(F: float, K: float, T: float, sigma: float, d1: float, d2: float, is_call: bool, r: float = interest_rate) -> float:
    return math.exp(-r * T) * norm.cdf(d1) if is_call \
           else -math.exp(-r * T) * norm.cdf(-d1)

@pw.udf
def compute_gamma(F: float, K: float, T: float, sigma: float, d1: float, d2: float, is_call: bool, r: float = interest_rate) -> float:
    return math.exp(-r * T) * norm.pdf(d1) / (F * sigma * math.sqrt(T))

@pw.udf
def compute_theta(F: float, K: float, T: float, sigma: float, d1: float, d2: float, is_call: bool, r: float = interest_rate) -> float:
    return (-F * sigma * norm.pdf(d1) / (2 * math.sqrt(T)) - r * K * math.exp(-r * T) * norm.cdf(d2) + r * F * math.exp(-r * T) * norm.cdf(d1)) / 252 if is_call \
           else (-F * sigma * norm.pdf(d1) / (2 * math.sqrt(T)) + r * K * math.exp(-r * T) * norm.cdf(-d2) - r * F * math.exp(-r * T) * norm.cdf(-d1)) / 252# per day

@pw.udf
def compute_vega(F: float, K: float, T: float, sigma: float, d1: float, d2: float, is_call: bool, r: float = interest_rate) -> float:
    return F * norm.pdf(d1) * math.sqrt(T) * math.exp(-r * T) / 100

@pw.udf
def compute_rho(F: float, K: float, T: float, sigma: float, d1: float, d2: float, is_call: bool, r: float = interest_rate) -> float:
    return -T * math.exp(-r * T) * (F * norm.cdf(d1) - K * norm.cdf(d2)) / 100 if is_call \
           else -T * math.exp(-r * T) * (K * norm.cdf(-d2) - F * norm.cdf(-d1)) / 100


# -

# ### Computing the Options Greeks on the data
#
# All you need to do is to use the UDFs you have just defined.
#
# First, let's add a column to determine if the option is a call or a put:

table_texp = table_texp.with_columns(is_call=pw.this.instrument_class == 'C')

# Now, you can start computing the Options Greeks. First, let's start with the implied volatility, pre-filtering.

table_volatility_unfiltered = table_texp.with_columns(
    volatility=compute_volatility(
        pw.this.future_price,
        pw.this.strike_price,
        pw.this.time_to_expiration,
        pw.this.is_call,
        pw.this.option_midprice
    )
)

# Filter out entries were volatility couldn't be computed.

table_sigma = table_volatility_unfiltered.filter(pw.this.volatility.is_not_none())
# _MD_COMMENT_START_
pw.debug.compute_and_print(table_sigma, n_rows=5)
# _MD_COMMENT_END_

# Having filtered the volatility table, you can now compute the useful variables $d_1, d_2$.

# +
table_d1d2 = table_sigma.with_columns(
    d1=compute_d1(pw.this.future_price, pw.this.strike_price, pw.this.time_to_expiration, pw.this.volatility),
    d2=compute_d2(pw.this.future_price, pw.this.strike_price, pw.this.time_to_expiration, pw.this.volatility)
)


# _MD_COMMENT_START_
pw.debug.compute_and_print(table_d1d2, n_rows=5)
# _MD_COMMENT_END_
# -

# And finally, you have everything necessary to compute the Option Greeks.

# +
table_greeks = table_d1d2.select(
    ts_recv=pw.this.ts_recv,
    instrument_id=pw.this.instrument_id, # option identifier
    delta=compute_delta(F=pw.this.future_price, K=pw.this.strike_price, T=pw.this.time_to_expiration,
                        sigma=pw.this.volatility, d1=pw.this.d1, d2=pw.this.d2, is_call=pw.this.is_call),
    gamma=compute_gamma(F=pw.this.future_price, K=pw.this.strike_price, T=pw.this.time_to_expiration,
                        sigma=pw.this.volatility, d1=pw.this.d1, d2=pw.this.d2, is_call=pw.this.is_call),
    theta=compute_theta(F=pw.this.future_price, K=pw.this.strike_price, T=pw.this.time_to_expiration,
                        sigma=pw.this.volatility, d1=pw.this.d1, d2=pw.this.d2, is_call=pw.this.is_call),
    vega=compute_vega(F=pw.this.future_price, K=pw.this.strike_price, T=pw.this.time_to_expiration,
                      sigma=pw.this.volatility, d1=pw.this.d1, d2=pw.this.d2, is_call=pw.this.is_call),
    rho=compute_rho(F=pw.this.future_price, K=pw.this.strike_price, T=pw.this.time_to_expiration,
                    sigma=pw.this.volatility, d1=pw.this.d1, d2=pw.this.d2, is_call=pw.this.is_call),
)

# _MD_COMMENT_START_
pw.debug.compute_and_print(table_greeks, n_rows=5)
# _MD_COMMENT_END_
# -

# ### Output
#
# Now that you have successfully computed the Option Greeks, you can output the results to your favorite system.
# Pathway supports many different [connectors](/developers/user-guide/connect/pathway-connectors/).
#
# As an example, you might want to send the results to a CSV file, using Pathway CSV output connector:

pw.io.csv.write(table_greeks, "./options-greeks.csv")

# Now, all you need it to run the computation:

# +
#_MD_SHOW_pw.run()
# -

# The Options Greeks will be computed and stored in the `option-greeks.csv` file.
#
# #### Streamlit User Interface
#
# For a more user-friendly output, you can also output the data to a dashboard to visualize your results.
# As an example, you can easily set up a dashboard using Streamlit:
#
# ![Streamlit User Interface](/assets/content/showcases/option-greeks/Streamlit.png)
#
#
# You can find the sources to obtain this dashboard in our [public GitHub repository](https://github.com/pathwaycom/pathway/tree/main/examples/projects/option-greeks).

# ## Going live
#
# Pathway has a unified engine capable of processing both static and streaming data, making it easy to transition from one mode to the other.
# You can easily make the book orders dynamic by updating the input connector `ConnectorSubject` (`MBP1Subject`) to **simulate** real-time data streaming by adding a `time.sleep()` function call after each `next` call.
# This small modification introduces a delay between data points, emulating the arrival of new data over time.
# The updated source is available in our [public GitHub repository](https://github.com/pathwaycom/pathway/tree/main/examples/projects/option-greeks).
#
# In this case, Pathway will update the results every time the input changes, at the reception of new data point from the _mbp-1_ data for example.
#
# Furthermore, you can use _[Databento live APIs](https://databento.com/docs/api-reference-live?historical=python&live=python)_ to obtain the market live data for the book orders and have the Option Greeks updated in real-time as the live data is ingested. This way, you can make full use of the streaming mode.

# ## Conclusions
#
# Congratulations! You now are able to compute the _implied volatility_ and the _Option Greeks_ using Databento to extract the historical market data and Pathway to process it.
# Pathway is the ideal tool for quantitative projects, allowing you to compute complex financial metrics like Options Greek in real-time.
# If you are interested, check our example about [Bollinger Bands](/developers/templates/etl/live_data_jupyter) or reach out to us on [Discord](https://discord.com/invite/pathway)!
#
# ## Acknowledgements
#
# We would like to express our gratitude to Databento for their valuable help and support in the creation of this article.
