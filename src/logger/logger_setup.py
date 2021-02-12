"""
Author:     David Walshe
Date:       12 February 2021
"""

import os
import yaml
import logging.config

# Get the logger config in the same directory as this file.
config_file_path = os.path.join(os.path.dirname(__file__), "logger_config.yml")

with open(config_file_path, "r") as fh:
    config = yaml.safe_load(fh.read())

    logging.config.dictConfig(config)
