import logging
import logging.config
import json
import os


def setup_logging(
    default_path="logging.json", default_level=logging.INFO, env_key="LOG_CFG"
):
    """Setup logging configuration"""
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, "rt") as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


setup_logging()

# import my_module

logger = logging.getLogger(__name__)

# def main():
# logger.info("Starting main function")
# my_module.some_function()

# if __name__ == "__main__":
#     main()
