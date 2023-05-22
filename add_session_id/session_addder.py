import numpy as np
import pandas as pd
from pandas import DataFrame

from add_session_id.utils import logger_dec


class SessionAdder:
    used_sids: int = 0

    @classmethod
    def _add_sid_to_group(cls: "SessionAdder", group: DataFrame) -> DataFrame:
        """
        Condition: if the difference between the visits is >3min, this is the beginning of the next session.\n
        np.where() puts 1 in the cell where this condition is true, so cumsum() will be 1 more for all next cells.\n
        Parameters
        ----------
        group : DataFrame
            group of rows that were selected by df.groupby
        """

        group = group.sort_values(by=["timestamp"])

        group["session_id"] = (
            np.where(group["timestamp"] - group["timestamp"].shift(1) > pd.Timedelta(minutes=3), 1, 0).cumsum()
            + cls.used_sids
        )
        # последний sid + 1 ,тк новый кастомер должен начинаться с новой сессии
        cls.used_sids = group.iat[-1, group.columns.get_loc("session_id")] + 1

        return group

    @classmethod
    @logger_dec("adding 'session_id' column")
    def add_session_id(cls: "SessionAdder", df: DataFrame) -> DataFrame:
        """
        Сalculate and add column 'session_id' to dataframe\n
        Parameters
        ----------
        df : DataFrame
            dataframe to which a column should be added
        """

        cls.used_sids = 0
        # Сортируем по индексу что бы сохранить изначальный порядок записей
        return df.groupby("customer_id", group_keys=False).apply(cls._add_sid_to_group).sort_index()
