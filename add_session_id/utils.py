from datetime import datetime
from functools import wraps
from pathlib import Path

import numpy as np
import pandas as pd
from pandas import DataFrame

from add_session_id.config import logger


def logger_dec(func_info: str) -> callable:
    """
    Add start/finish logging to function\n
    Parameters
    ----------
    func_info : str
        string which will be displayed in the logs. For example "finish {func_info}"
    """

    def decorator(func: callable) -> callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            logger.info(f"start {func_info}")

            res = func(*args, **kwargs)

            logger.info(f"finish {func_info} ({datetime.now() - start_time})")

            return res

        return wrapper

    return decorator


@logger_dec("creating df")
def generate_df(
    n_customers: int = 1_000,
    n_products: int = 1_000_000,
    n_rows: int = 100_000_000,
    start: str = "2022-01-01 00:00:00",
    end: str = "2023-01-01 00:00:00",
    file_path: str = "./add_session_id/data/data.csv",
    save_to_file: bool = True,
) -> DataFrame:

    logger.debug(f"{n_customers=}, {n_products=}, {n_rows=}, {start=}, {end=}, {file_path=}")

    random_data = {
        "customer_id": np.random.randint(1, n_customers + 1, size=n_rows, dtype="uint32"),
        "product_id": np.random.randint(1, n_products + 1, size=n_rows, dtype="uint64"),
        "timestamp": pd.to_datetime(
            np.random.randint(
                pd.to_datetime(start).value // 10 ** 9,
                pd.to_datetime(end).value // 10 ** 9,
                n_rows,
            ),
            unit="s",
        ),
    }
    df = pd.DataFrame(random_data)

    if save_to_file:
        df.to_csv(file_path, index=False)

    return df


@logger_dec("reading from csv")
def read_df(file_path: Path = Path("./add_session_id/data/data.csv"), n_rows: int = 100_000_000) -> DataFrame:
    logger.debug(f"{file_path=}, {n_rows=}")

    return pd.read_csv(
        file_path,
        nrows=n_rows,
        dtype={"customer_id": "uint32", "product_id": "uint64"},
        parse_dates=["timestamp"],
    )


@logger_dec("writing to csv")
def write_to_csv(df: DataFrame, output_file_path: Path = Path("./add_session_id/data/new_data.csv")) -> None:
    df.to_csv(output_file_path, index=False)


# df = generate_df(
#         n_customers = 2,
#         n_products = 10,
#         n_rows = 10,
#         start = "2022-01-01 00:00:00",
#         end = "2022-01-01 00:10:00",
#         file_path = "./add_session_id/data/test_data.csv",
#         save_to_file = False,
#     )

# write_to_csv(df.sort_values(["customer_id", "timestamp"]), "/home/roman/add_session_id_project/tests/tests_data/test_data.csv")
