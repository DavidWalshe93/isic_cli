"""
Author:     David Walshe
Date:       12 February 2021
"""

import logging
from functools import wraps

logger = logging.getLogger(__name__)


def kwargs_to_namedtuple(named_tuple):
    """
    Converts a click commands kwargs into a named Tuple.

    :param named_tuple: A named tuple reference to create and inject as the command's param.
    """

    def _kwargs_to_namedtuple(func: callable):
        @wraps(func)
        def _kwargs_to_namedtuple_wrapper(*args, **kwargs):
            command_args = named_tuple(**kwargs)

            return func(command_args)

        return _kwargs_to_namedtuple_wrapper

    return _kwargs_to_namedtuple
