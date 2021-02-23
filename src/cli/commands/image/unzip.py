"""
Author:     David Walshe
Date:       23 February 2021
"""

import os
import logging
from zipfile import ZipFile
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, as_completed, Future

import click
from alive_progress import alive_bar

from src.cli.config import COMMAND_CONTEXT_SETTINGS
from src.cli.utils import kwargs_to_namedtuple
from src.cli.validators import check_file_exists

logger = logging.getLogger(__name__)

UnzipCommandParameters = namedtuple("UnzipCommandParameter", ["zip_dir", "output", "workers"])


@click.command(**COMMAND_CONTEXT_SETTINGS, short_help="Unzips and collects all images into a single directory.")
@click.option("--zip-dir", type=str, required=True, callback=check_file_exists, help="The previously downloaded metadata file.")
@click.option("-o", "--output", type=str, default=os.path.abspath("./isic_images_extracted"), help="The name of the directory to collect images to after unzipping.")
@click.option("-w", "--workers", type=int, default=5, help=f"Specify how many concurrent workers should be used. Default=5")
@kwargs_to_namedtuple(UnzipCommandParameters)
def unzip(params: UnzipCommandParameters):
    """

    :param params:
    :return:
    """
    # Extraction path.
    create_extraction_path(params)
    # Unzip archive.
    unzip_images(params)


def create_extraction_path(params: UnzipCommandParameters) -> None:
    """
    Creates the download folder if it doesnt already exists.

    :param params: The command line parameters.
    """
    if not os.path.exists(params.output):
        os.makedirs(params.output)


def unzip_images(params: UnzipCommandParameters):
    """
    Uses a thread pool to download image metadata concurrently.

    :param params: The CLI parameters.
    :return: The tuple of metadata results and names of images that failed to download.
    """
    # Obtain all archive files in the zip directory.
    zip_archives = [os.path.abspath(os.path.join(params.zip_dir, archive)) for archive in os.listdir(params.zip_dir) if archive.endswith(".zip")]
    num_archives = len(zip_archives)

    # First archive is done sequentially to stop OSErrors when creating file structures for datasets.
    unzip_archive(zip_archives.pop(), params)

    # Measure the download progress for the full dataset.
    with alive_bar(num_archives, title="Total Progress", enrich_print=False) as total_bar:
        # Account for the first sequential
        total_bar()
        # Run concurrent workers to download ìmages.
        with ThreadPoolExecutor(max_workers=params.workers) as executor:
            # Create a worker with a set of images to request and download.
            futures_to_request = {executor.submit(unzip_archive, archive, params): archive for batch_idx, archive in enumerate(zip_archives)}
            for index, future in enumerate(as_completed(futures_to_request)):
                try:
                    future.result()
                    total_bar()
                except Exception as e:
                    logger.error(f"{e}")
                    logger.error(f"{futures_to_request[future]}")


def unzip_archive(archive: str, params: UnzipCommandParameters) -> None:
    """
    Unzip archive and place contents into output directory.

    :param archive: The archive to read data from.
    :param params: The CLI parameters.
    """
    with ZipFile(archive, 'r') as zip_ref:
        zip_ref.extractall(params.output)
