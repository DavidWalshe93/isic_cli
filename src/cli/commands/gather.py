"""
Author:     David Walshe
Date:       13 February 2021
"""

import os
import json
import logging
from collections import namedtuple

import click

from src.cli.config import COMMAND_CONTEXT_SETTINGS
from src.cli.utils import kwargs_to_namedtuple

logger = logging.getLogger(__name__)

GatherCommandParams = namedtuple("GatherCommandParams", ["path", "output"])


@click.command(**COMMAND_CONTEXT_SETTINGS, short_help="")
@click.option("-p", "--path", type=str, required=True, help="")
@click.option("-o", "--output", type=str, required=True, help="")
@kwargs_to_namedtuple(GatherCommandParams)
def gather(params: GatherCommandParams):
    """

    :param params:
    :return:
    """
    files = os.listdir(params.path)

    with open(params.output, "w") as fh:
        json.dump(files, fh, indent=4)
