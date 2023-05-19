import argparse
import sys
from pathlib import Path

from config.config import env_dict, logger

from add_session_id.session_addder import SessionAdder
from add_session_id.utils import generate_df, read_df, write_to_csv
import json


def createParser():
    parser = argparse.ArgumentParser()
    for argument in [("-sres", "--saveres"), ("-sgen", "--savegen"), ("-ff", "--fromfile")]:
        parser.add_argument(argument[0], argument[1])

    return parser


if __name__ == "__main__":
    namespace = createParser().parse_args(sys.argv[1:])

    df_from_file = json.loads(namespace.fromfile.lower())
    save_gen_df = json.loads(namespace.savegen.lower())
    save_res_df = json.loads(namespace.saveres.lower())

    n_customers = int(env_dict.get("N_CUSTOMERS"))
    n_products = int(env_dict.get("N_PRODUCTS"))
    n_rows = int(env_dict.get("N_ROWS"))
    start = env_dict.get("START_TIME")
    end = env_dict.get("END_TIME")
    input_file_path = Path(env_dict.get("INPUT_FILE_PATH", "./add_session_id/data/data.csv"))
    output_file_path = Path(env_dict.get("OUTPUT_FILE_PATH", "./add_session_id/data/new_data.csv"))

    if df_from_file:
        df = read_df(input_file_path, n_rows)
    else:
        df = generate_df(n_customers, n_products, n_rows, start, end, input_file_path, save_to_file=save_gen_df)

    df = SessionAdder.add_session_id(df)
    logger.info(f"\n{df}")

    if save_res_df:
        write_to_csv(df, output_file_path)
