import traceback
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
            # get real file and line what was called
            file = "/".join(traceback.extract_stack()[-2].filename.split("/")[-2:])
            function_name = func.__name__
            line = traceback.extract_stack()[-2].lineno

            start_time = datetime.now()
            logger.patch(lambda r: r.update(function=function_name, name=file, line=line)).info(f"start {func_info}")

            res = func(*args, **kwargs)

            logger.patch(lambda r: r.update(function=function_name, name=file)).info(
                f"finish {func_info} ({datetime.now() - start_time})"
            )

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
    save_to_file: bool = False,
    file_path: str = "./data/data_generated.csv",
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

    logger.debug(f"\n{df}")

    if save_to_file:
        df.to_csv(file_path, index=False)

    return df


@logger_dec("reading from csv")
def read_df(file_path: Path = Path("./data/data.csv"), n_rows: int = 100_000_000) -> DataFrame:
    logger.debug(f"{file_path=}, {n_rows=}")

    return pd.read_csv(
        file_path,
        nrows=n_rows,
        dtype={"customer_id": "uint32", "product_id": "uint64"},
        parse_dates=["timestamp"],
    )


@logger_dec("writing to csv")
def write_to_csv(df: DataFrame, output_file_path: Path = Path("./data/new_data.csv")) -> None:
    df.to_csv(output_file_path, index=False)
