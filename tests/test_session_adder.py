from pathlib import Path
from time import time

import matplotlib.pyplot as plt
import pandas as pd
import pytest

from add_session_id.config import logger
from add_session_id.session_addder import SessionAdder
from add_session_id.utils import generate_df, read_df

DATA_FILE_PATH = Path(__file__).parents[0] / "data"


def save_fig(data: dict) -> None:
    fig, axis = plt.subplots()

    axis.plot(data.keys(), data.values(), linewidth=1)
    axis.title.set_text("Time complexity test")
    axis.title.set_fontsize(14)
    axis.set_xlabel("N rows")
    axis.set_ylabel("Time (ms)")

    fig.savefig(DATA_FILE_PATH / "time_test.png", dpi=100)


@pytest.fixture(autouse=False)
def gen_df_fixture():

    path = DATA_FILE_PATH / "temp.csv"
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

    df: pd.DataFrame
    path: Path
    n_rows: int

    assert df.shape[0] == n_rows
    assert pd.Timestamp(min(df["timestamp"])) >= pd.Timestamp("2022-01-01 00:00:00")
    assert pd.Timestamp(max(df["timestamp"])) <= pd.Timestamp("2023-01-01 00:00:00")
    assert (df[["customer_id", "product_id"]] > 0).any().any()
    assert df.equals(read_df(file_path=path, n_rows=n_rows))


def test_session_adder_alorhythm():
    """Testing the correctness of the alorhythm"""
    df = read_df(DATA_FILE_PATH / "alorhythm_test_before.csv", n_rows=10)
    after_df = read_df(DATA_FILE_PATH / "alorhythm_test_after.csv", n_rows=10)

    assert after_df.equals(SessionAdder.add_session_id(df))


def test_session_adder_time_complexity():
    """Testing time complexity. From 10 mil. to 100 mil. row"""
    observations = {}

    for i in range(1, 11):

        n_rows = int(i * 1e7)
        start = time()

        df = generate_df(
            n_customers=int(n_rows / 100_000),
            n_products=int(n_rows / 100),
            n_rows=n_rows,
            start="2022-01-01 00:00:00",
            end="2023-01-01 00:00:00",
        )

        df = SessionAdder.add_session_id(df)

        time_diff = int((time() - start) * 10 ** 3)

        logger.info(f"{n_rows=} done, time: {time_diff} ms \n")
        observations[format(n_rows, ".0e")] = time_diff

    save_fig(observations)
