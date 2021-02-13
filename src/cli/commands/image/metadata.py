"""
Author:     David Walshe
Date:       12 February 2021
"""

import logging
import json
from collections import namedtuple

import click

from src.api.isic_api import IsicApi
from src.cli.config import COMMAND_CONTEXT_SETTINGS
from src.cli.utils import kwargs_to_namedtuple

logger = logging.getLogger(__name__)

MetadataCommandParameters = namedtuple("MetadataCommandParameters", ["input"])


@click.command(**COMMAND_CONTEXT_SETTINGS, short_help="Download metadata for a list of images.")
@click.option("-i", "--input", type=str, required=True, help="The path to the directory to take image names from.")
@kwargs_to_namedtuple(MetadataCommandParameters)
def metadata(params: MetadataCommandParameters):
    """

    :param params:
    :return:
    """
    with open(params.input) as fh:
        image_ids = json.load(fh)

    api = IsicApi()

    for image in image_ids:
        endpoint = f"image/{image}"

        res = api.get_json(endpoint)

        print(res)