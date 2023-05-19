from datetime import datetime
from functools import wraps
from pathlib import Path

import numpy as np
import pandas as pd
from pandas import DataFrame

from add_session_id.config.config import logger


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
    n_customers: int, n_products: int, n_rows: int, start: str, end: str, file_path: str, save_to_file: bool = True
) -> None:

    logger.debug(f"{n_customers=}, {n_products=}, {n_rows=}, {start=}, {end=}, {file_path=}")

    random_data = {
        "customer_id": np.random.randint(1, n_customers + 1, size=n_rows, dtype="uint32"),
        "product_id": np.random.randint(1, n_products + 1, size=n_rows, dtype="uint64"),
        "timestamp": pd.to_datetime(
            np.random.randint(pd.to_datetime(start).value // 10 ** 9, pd.to_datetime(end).value // 10 ** 9, n_rows),
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
