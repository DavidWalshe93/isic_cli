"""
Author:     David Walshe
Date:       12 February 2021
"""

import logging
from collections import namedtuple

import click
from click.core import Context

from src.cli.config import GROUP_CONTEXT_SETTINGS
from src.cli.utils import kwargs_to_namedtuple
from src.cli.validators import convert_bool_to_lower

# Commands
from src.cli.commands.image.metadata import metadata
from src.cli.commands.image.download import download
from src.cli.commands.image.unzip import unzip

from src.api.isic_api import IsicApi

logger = logging.getLogger(__name__)

ImageCommandParameters = namedtuple("ImageCommandParameters", ["limit", "offset", "sort", "desc", "detail", "name", "timeout"])


@click.group(**GROUP_CONTEXT_SETTINGS, short_help="Downloads an image")
@click.option("-l", "--limit", type=int, default=50, help="Result set size limit.")
@click.option("-o", "--offset", type=int, default=0, help="Offset into result set.")
@click.option("-s", "--sort", type=str, default="name", help="Field to sort the result set by.")
@click.option("--desc", is_flag=True, help="Sort order: 1 for ascending, -1 for descending.")
@click.option("--detail", is_flag=True, callback=convert_bool_to_lower, help="Display the full information for each image, instead of a summary.")
@click.option("--name", type=str, default="", help="Find an image with a specific name.")
@click.option("--timeout", type=int, default=5, help="The request timeout length in seconds.")
# @click.option("-f", "--filter", type=str, default="", help="Filter the images by a PegJS-specified grammar.")
@kwargs_to_namedtuple(ImageCommandParameters)
@click.pass_context
def image(ctx: Context, params: ImageCommandParameters):
    """
    Calls the "<api>/image" endpoint
    """
    if ctx.invoked_subcommand is None:
        api = IsicApi()

        endpoint = f"image?" \
                   f"sort={params.sort}&" \
                   f"sortdir={-1 if params.desc else 1}&" \
                   f"detail={params.detail}"

        # Add name check if specified
        endpoint += f"&name={params.name}" if params.name != "" else ""

        print(len(list(api.get_json_list(endpoint=endpoint, limit=params.limit, offset=params.offset, timeout=params.timeout))))
        print(list(api.get_json_list(endpoint=endpoint, limit=params.limit, offset=params.offset, timeout=params.timeout)))

        # for item in api.get_json_list(endpoint=endpoint, limit=params.limit, offset=params.offset):
        #     print(item)

# ==================================================
# Add CLI commands
# ==================================================
commands = [
    metadata,
    download,
    unzip
]

for command in commands:
    image.add_command(command)
