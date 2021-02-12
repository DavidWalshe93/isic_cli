"""
Author:     David Walshe
Date:       12 February 2021
"""

import logging
from collections import namedtuple

import click

from src.cli.config import GROUP_CONTEXT_SETTINGS
from src.cli.utils import kwargs_to_namedtuple

from src.api.isic_api import IsicApi

logger = logging.getLogger(__name__)

ImageCommandParameters = namedtuple("ImageCommandParameters", ["id"])


@click.group(**GROUP_CONTEXT_SETTINGS, short_help="Downloads an image")
@click.option("-i", "--id", type=str, required=True, help="")
@kwargs_to_namedtuple(ImageCommandParameters)
def image(params: ImageCommandParameters):
    """

    :param params: The command parameters as a named tuple.
    """
    print(params.id)
    api = IsicApi()

    # api.get()


