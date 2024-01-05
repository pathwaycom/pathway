# Copyright © 2024 Pathway

import pandas as pd

import pathway as pw
from pathway.tests.utils import (
    T,
    assert_table_equality_wo_index,
    assert_table_equality_wo_index_types,
)


def get_movement_totals(table_transactions):
    results = table_transactions.groupby(
        table_transactions.sender, table_transactions.receiver
    ).reduce(
        user_A=table_transactions.sender,
        user_B=table_transactions.receiver,
        usd_total_estimate=pw.reducers.sum(table_transactions.usd_estimate),
    )
    return results


# DO NOT MODIFY THIS WITHOUT MODIFYING the following file:
# public/website3/content/2.developers/6.tutorials/.user_pairs_fuzzy_join/article.py # noqa E501
def perform_user_pairs_match(eth_transactions, btc_transactions):
    # ETH movements one way
    eth_movement_totals = eth_transactions.groupby(
        eth_transactions.sender, eth_transactions.receiver
    ).reduce(
        user_A=eth_transactions.sender,
        user_B=eth_transactions.receiver,
        usd_total_estimate=pw.reducers.sum(eth_transactions.usd_estimate),
    )
    # BTC movements the other way
    btc_movement_totals = btc_transactions.groupby(
        btc_transactions.sender, btc_transactions.receiver
    ).reduce(
        user_A=btc_transactions.receiver,
        user_B=btc_transactions.sender,
        usd_total_estimate=pw.reducers.sum(btc_transactions.usd_estimate),
    )

    # We run fuzzy join on the two aggregate transaction tables.
    left_projection = {"user_A": "C1", "user_B": "C2"}
    right_projection = {"user_A": "C1", "user_B": "C2"}
    matches = pw.ml.smart_table_ops.fuzzy_match_tables(
        eth_movement_totals,
        btc_movement_totals,
        left_projection=left_projection,
        right_projection=right_projection,
    )
    matched_users = matches.select(
        btc_sender=btc_movement_totals.ix(matches.right).user_B,
        btc_receiver=btc_movement_totals.ix(matches.right).user_A,
        eth_sender=eth_movement_totals.ix(matches.left).user_A,
        eth_receiver=eth_movement_totals.ix(matches.left).user_B,
        confidence=matches.weight,
    )

    return matched_users


def test_get_movement_totals():
    eth_transactions = T(
        """
      | sender | receiver | usd_estimate
    1 | A      | B        | 1
    2 | A      | B        | 10
    3 | A      | B        | 100
    4 | C      | D        | 1000
    """
    )
    assert_table_equality_wo_index(
        get_movement_totals(eth_transactions),
        T(
            """
      | user_A | user_B | usd_total_estimate
    1 | A      | B        | 111
    2 | C      | D        | 1000
    """
        ),
    )


def test_gist_single_match():
    eth_transactions = T(
        """
      | sender | receiver | usd_estimate
    1 | A      | B        | 1
    2 | A      | B        | 10
    3 | A      | B        | 100
    """
    )
    btc_transactions = T(
        """
      | sender | receiver | usd_estimate
    1 | B      | A        | 1000
    """
    )
    results = perform_user_pairs_match(eth_transactions, btc_transactions)
    expected_results = T(
        """
      | btc_sender | btc_receiver | eth_sender | eth_receiver | confidence
    1 | B          | A            | A          | B            | 1
    """
    )
    assert_table_equality_wo_index_types(
        results,
        expected_results,
    )


def test_gist_identical_tables():
    eth_transactions = T(
        """
      | sender | receiver | usd_estimate
    1 | A      | B        | 1
    """
    )
    btc_transactions = T(
        """
      | sender | receiver | usd_estimate
    1 | A      | B        | 1
    """
    )
    assert_table_equality_wo_index_types(
        perform_user_pairs_match(eth_transactions, btc_transactions),
        T(
            pd.DataFrame(
                {
                    "btc_sender": [],
                    "btc_receiver": [],
                    "eth_sender": [],
                    "eth_receiver": [],
                    "confidence": [],
                }
            ),
            format="pandas",
        ),
    )


def test_gist_small_tables():
    eth_transactions = T(
        """
      | sender | receiver | usd_estimate
    1 | A      | B        | 1
    2 | A      | B        | 10
    3 | A      | B        | 100
    4 | C      | D        | 1000
    """
    )
    btc_transactions = T(
        """
      | sender | receiver | usd_estimate
    1 | C      | E        | 1
    2 | A      | F        | 10
    3 | B      | A        | 100
    4 | C      | D        | 1000
    """
    )
    results = perform_user_pairs_match(eth_transactions, btc_transactions)
    expected_results = T(
        """
      | btc_sender | btc_receiver | eth_sender | eth_receiver | confidence
    1 | B          | A            | A          | B            | 1.0
    """
    )
    assert_table_equality_wo_index_types(results, expected_results)
