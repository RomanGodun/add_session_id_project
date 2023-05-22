from pathlib import Path

from config import env_dict, logger
import pydantic
from add_session_id.session_addder import SessionAdder
from add_session_id.utils import generate_df, read_df, write_to_csv

if __name__ == "__main__":

    DF_FROM_FILE = pydantic.parse_obj_as(bool, env_dict.get("DF_FROM_FILE", "False"))

    # params for read_df()
    INPUT_FILE_PATH = Path(env_dict.get("INPUT_FILE_PATH"))

    # params for generate_df()
    N_CUSTOMER = int(env_dict.get("N_CUSTOMERS"))
    N_PRODUCTS = int(env_dict.get("N_PRODUCTS"))
    N_ROWS = int(env_dict.get("N_ROWS"))
    START_TIME: str = env_dict.get("START_TIME")
    END_TIME: str = env_dict.get("END_TIME")

    SAVE_GEN_DF = pydantic.parse_obj_as(bool, env_dict.get("SAVE_GEN_DF", "False"))
    SAVE_GEN_FILE_PATH = Path(env_dict.get("SAVE_GEN_FILE_PATH"))

    # params for write_to_csv()
    SAVE_RES_DF = pydantic.parse_obj_as(bool, env_dict.get("SAVE_RES_DF", "False"))
    OUTPUT_FILE_PATH = Path(env_dict.get("OUTPUT_FILE_PATH"))

    if DF_FROM_FILE:
        df = read_df(INPUT_FILE_PATH, N_ROWS)
    else:
        df = generate_df(N_CUSTOMER, N_PRODUCTS, N_ROWS, START_TIME, END_TIME, SAVE_GEN_DF, SAVE_GEN_FILE_PATH)

    df = SessionAdder.add_session_id(df)
    logger.info(f"\n{df}")

    if SAVE_RES_DF:
        write_to_csv(df, OUTPUT_FILE_PATH)
