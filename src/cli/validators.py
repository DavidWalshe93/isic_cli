"""
Author:     David Walshe
Date:       12 February 2021
"""

import logging

logger = logging.getLogger(__name__)


def convert_bool_to_lower(ctx, param, value: bool) -> str:
    """
    Converts the passed Python boolean value to lower case i.e. False -> false
    to meet API lowercase requirements.

    :param value: The boolean value to convert
    :return: The value as a string lowercase.
    """
    return str(value).lower()