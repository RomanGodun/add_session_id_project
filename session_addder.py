from pathlib import Path

import numpy as np
import pandas as pd
from pandas import DataFrame

from config.utils import env_dict, logger, logger_dec


class SessionAdder:
    used_sid: int = 0

    @classmethod
    def _add_sid_to_group(cls: "SessionAdder", group: DataFrame) -> DataFrame:

        group = group.sort_values(by=["timestamp"])
        # Условие: если разница между посещениями >3мин то это начало следующей сесии.
        # np.where ставит в ячейку в которой сработало это условие - 1, а значит cumsum() будет на 1 больше для всех следующих ячеек
        # id сессии должны быть уникальными между разными группами (кастомерами). Поэтому мы прибавляем к массиву used_sid (сколько sid было занято до этого)
        group["session_id"] = (
            np.where(group["timestamp"] - group["timestamp"].shift(1) > pd.Timedelta(minutes=3), 1, 0).cumsum()
            + cls.used_sid
        )
        # забираем последний sid (кол-во всего использованных) и прибавляем 1 тк следующий sid у следующего кастомера должен отличаться
        cls.used_sid = group.iat[-1, group.columns.get_loc("session_id")] + 1

        return group

    @classmethod
    @logger_dec("adding 'session_id' column")
    def add_session_id(cls: "SessionAdder", df: DataFrame) -> DataFrame:

        # обновляем session id для нового датафрейма
        cls.used_sid = 0
        # Сортируем по индексу что б сохранить изначальный порядок записей
        return df.groupby("customer_id", group_keys=False).apply(cls._add_sid_to_group).sort_index()

    @staticmethod
    @logger_dec("creating csv")
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

    @staticmethod
    @logger_dec("reading from csv")
    def read_df(file_path: Path = Path("./data/data.csv"), n_rows: int = 100_000_000) -> DataFrame:
        logger.debug(f"{input_file_path=}, {n_rows=}")

        return pd.read_csv(
            file_path,
            nrows=n_rows,
            dtype={"customer_id": "uint32", "product_id": "uint64"},
            parse_dates=["timestamp"],
        )

    @staticmethod
    @logger_dec("writing to csv")
    def write_to_csv(df: DataFrame, output_file_path: Path = Path("./data/new_data.csv")) -> None:
        df.to_csv(output_file_path, index=False)


if __name__ == "__main__":

    # Ниже пример использования класса SessionApplyer:

    n_customers = int(env_dict.get("N_CUSTOMERS"))
    n_products = int(env_dict.get("N_PRODUCTS"))
    n_rows = int(env_dict.get("N_ROWS"))
    start = env_dict.get("START_TIME")
    end = env_dict.get("END_TIME")
    input_file_path = Path(env_dict.get("INPUT_FILE_PATH", "./data/data.csv"))
    output_file_path = Path(env_dict.get("OUTPUT_FILE_PATH", "./data/new_data.csv"))

    # Генерируем датафрейм, save_to_file - сохранять или нет в csv
    df = SessionAdder.generate_df(n_customers, n_products, n_rows, start, end, input_file_path, save_to_file=False)

    # Из файла
    # df = SessionApplyer.read_df(input_file_path, n_rows)

    # Добавляем колонку
    df = SessionAdder.add_session_id(df)

    # Результирующий датафрейм
    logger.info(f"\n{df}")

    # Записываем результат в файл
    # SessionAdder.write_to_csv(df, output_file_path)
