import os
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime
from black_interface.utils import get_env_dict, get_logger
from dotenv import dotenv_values
from loguru import logger
from functools import wraps

def get_env_dict(env_dir: Optional[Path]):
    env_shared = dotenv_values(str(env_dir / "shared.env"))
    env_secret = dotenv_values(str(env_dir / "secret.env"))
    env_dict = {**env_shared, **env_secret, **os.environ}
    env_dict = {
        k: v
        for k, v in env_dict.items()
        if ("<secret>" not in v) or ("<project>" not in v)
    }
    return env_dict


def get_logger(
    logging_level: str, log_dir: Optional[Path] = None, logging_dir_level: str = "DEBUG"
):
    logger.remove()
    logger.add(sys.stderr, level=logging_level)

    # если указать путь, то логи будут записываться не только в std.err
    if log_dir:
        logger.add(log_dir / "{time}.log", level=logging_dir_level)

    return logger


env_dict = get_env_dict(Path(__file__).parent)
logger = get_logger(
    logging_level=env_dict.get("LOGGING_LEVEL", "INFO"),
    log_dir=env_dict.get("LOGGING_DIR_LEVEL", None),
    logging_dir_level=env_dict.get("LOGGING_DIR_LEVEL", "DEBUG"),
)


def logger_dec(func_info:str) -> callable:
    def decorator(func) -> callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            
            start_time = datetime.now()
            logger.info(f"start {func_info}")
            
            res = func(*args, **kwargs)
            
            logger.info(f"finish {func_info} ({datetime.now() - start_time})")
            
            return res
        return wrapper
    return decorator
