import os
import sys
from pathlib import Path
from typing import Optional
from dotenv import dotenv_values
from loguru import logger as base_logger


def get_env_dict(env_dir: Optional[Path]):
    env_shared = dotenv_values(str(env_dir / "shared.env"))
    env_secret = dotenv_values(str(env_dir / "secret.env"))
    env_dict = {**env_shared, **env_secret, **os.environ}
    env_dict = {k: v for k, v in env_dict.items() if ("<secret>" not in v) or ("<project>" not in v)}
    return env_dict


def get_logger(logging_level: str, log_dir: Optional[Path] = None, logging_dir_level: str = "DEBUG"):
    base_logger.remove()
    base_logger.add(sys.stderr, level=logging_level)

    # если указать путь, то логи будут записываться не только в stderr
    if log_dir:
        base_logger.add(log_dir / "{time}.log", level=logging_dir_level)

    return base_logger


env_dict = get_env_dict(Path(__file__).parent)
logger = get_logger(
    logging_level=env_dict.get("LOGGING_LEVEL", "INFO"),
    log_dir=env_dict.get("LOGGING_DIR_LEVEL", None),
    logging_dir_level=env_dict.get("LOGGING_DIR_LEVEL", "DEBUG"),
)
