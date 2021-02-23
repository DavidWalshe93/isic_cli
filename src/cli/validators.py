"""
Author:     David Walshe
Date:       12 February 2021
"""

import logging
import os

import click

logger = logging.getLogger(__name__)


def convert_bool_to_lower(ctx, param, value: bool) -> str:
    """
    Converts the passed Python boolean value to lower case i.e. False -> false
    to meet API lowercase requirements.

    :param value: The boolean value to convert
    :return: The value as a string lowercase.
    """
    return str(value).lower()


def check_file_exists(ctx, param, value: str) -> str:
    """
    Check if the file passed as a parameter exists on the filesystem.

    :raise BadParameter: If passed parameter does not exist.
    """
    if os.path.exists(value):
        return value
    else:
        raise click.BadParameter(f"'{value}' for parameter '--{param.name}' does not exist.")
