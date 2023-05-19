from pathlib import Path

import pandas as pd
import pytest

from add_session_id.session_addder import SessionAdder
from add_session_id.utils import generate_df, read_df


@pytest.fixture(autouse=False)
def gen_df_fixture():

    path = Path(__file__).parents[0] / "data/temp.csv"
    n_rows = 1_000_000

    df = generate_df(
        n_customers=1_000,
        n_products=1_000_000,
        n_rows=n_rows,
        start="2022-01-01 00:00:00",
        end="2023-01-01 00:00:00",
        file_path=path,
        save_to_file=True,
    )

    yield df, path, n_rows

    if path.exists():
        path.unlink()

    del df


def test_generate_df(gen_df_fixture):
    """Testing dataframe generator"""

    df, path, n_rows = gen_df_fixture

    assert df.shape[0] == n_rows
    assert pd.Timestamp(min(df["timestamp"])) >= pd.Timestamp("2022-01-01 00:00:00")
    assert pd.Timestamp(max(df["timestamp"])) <= pd.Timestamp("2023-01-01 00:00:00")
    assert (df[["customer_id", "product_id"]] > 0).any().any()
    assert df.equals(read_df(file_path=path, n_rows=n_rows))


def test_session_adder_small():
    """Testing the correctness of the alorhythm"""

    df = read_df(file_path=Path(__file__).parents[0] / "data/small_test_before.csv", n_rows=10)
    after_df = read_df(file_path=Path(__file__).parents[0] / "data/small_test_after.csv", n_rows=10)

    assert after_df.equals(SessionAdder.add_session_id(df))


def test_session_adder_large():
    """Testing the algorhythm on a large number of rows"""

    path = Path(__file__).parents[0] / "data/temp.csv"
    df = generate_df(
        n_customers=1_000,
        n_products=1_000_000,
        n_rows=100_000_000,
        start="2022-01-01 00:00:00",
        end="2023-01-01 00:00:00",
        file_path=path,
        save_to_file=False,
    )

    SessionAdder.add_session_id(df)
