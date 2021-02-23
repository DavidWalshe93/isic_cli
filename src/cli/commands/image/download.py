"""
Author:     David Walshe
Date:       21 February 2021
"""

import logging
from collections import namedtuple
from itertools import chain
from queue import Queue
from typing import List, Union
import os
import json
import urllib as urllib
from concurrent.futures import ThreadPoolExecutor, as_completed, Future

import click
import pandas as pd
from alive_progress import alive_bar

from src.cli.config import COMMAND_CONTEXT_SETTINGS
from src.cli.utils import kwargs_to_namedtuple
from src.cli.validators import check_file_exists
from src.api.isic_api import IsicApi

logger = logging.getLogger(__name__)

DownloadCommandParameters = namedtuple("DownloadCommandParameters", ["metadata_file", "dataset", "include", "output", "retry", "timeout", "workers"])


@click.command(**COMMAND_CONTEXT_SETTINGS, short_help="Download a group of images based on their ISIC imageIds.")
@click.option("--metadata-file", type=str, required=True, callback=check_file_exists, help="The previously downloaded metadata file.")
@click.option("--dataset", type=str, default=None, help="The dataset name to download images for.")
@click.option("--include", type=click.Choice(["all", "images", "metadata"], case_sensitive=False), default="images")
@click.option("-o", "--output", type=str, default="./isic_images", help="The name of the directory to save images to.")
@click.option("--retry", is_flag=True, help="Tries to download the missing records from a previous attempt.")
@click.option("--timeout", type=int, default=60, help="The timeout length for each request to the API. Default=60")
@click.option("-w", "--workers", type=int, default=5, help=f"Specify how many concurrent workers should be used. Default=5")
@kwargs_to_namedtuple(DownloadCommandParameters)
def download(params: DownloadCommandParameters):
    """

    :param params:
    :return:
    """
    # Show available datasets if non are selected.
    if params.dataset is None:
        show_available_datasets(params)

    else:
        # Create download directory
        create_download_path(params)

        # Create an error Queue
        error_queue = Queue()

        # Get the image ids to download
        download_images(params, error_queue)

        # Record failed images to allow for --retry.
        failed_images = []
        while not error_queue.empty():
            failed_images.append(error_queue.get())

        # flatten list to 1D.
        failed_images = list(chain.from_iterable(failed_images))

        # Save the image_ids to retry with --retry.
        with open(recovery_file_name(), "w") as fh:
            json.dump(failed_images, fh, indent=4)


def recovery_file_name():
    """The name of the recovery file to use for --retry"""
    return ".failed_download_batches.json"


def show_available_datasets(params: DownloadCommandParameters):
    """
    Returns the available datasets in the metadata passed.

    :return:
    """
    print(f"\nDatasets available in '{params.metadata_file}':\n")
    datasets = pd.read_csv(params.metadata_file)["dataset"]
    items = datasets.value_counts()
    print(pd.DataFrame({"Datasets": items.index,
                        "Instances": items.values}))


def create_download_path(params: DownloadCommandParameters) -> None:
    """
    Creates the download folder if it doesnt already exists.

    :param params: The command line parameters.
    """
    if not os.path.exists(params.output):
        os.makedirs(params.output)


def download_images(params: DownloadCommandParameters, error_queue: Queue):
    """
    Uses a thread pool to download image metadata concurrently.

    :param params: The CLI parameters.
    :param error_queue: A queue to capture errors.
    :return: The tuple of metadata results and names of images that failed to download.
    """
    api = IsicApi()

    # Max size of download set by ISIC API
    MAX_DOWNLOAD_SIZE = 300

    # Get all the image ids from the
    image_ids = get_image_ids(params)

    # Get all batches.
    image_batches = list(chunks(image_ids, MAX_DOWNLOAD_SIZE))

    # Measure the download progress for the full dataset.
    with alive_bar(len(image_batches), title="Total Progress", enrich_print=False) as total_bar:
        # Run concurrent workers to download ìmages.
        with ThreadPoolExecutor(max_workers=params.workers) as executor:
            # Create a worker with a set of images to request and download.
            futures_to_request = {executor.submit(make_request, api, batch, params): batch for batch_idx, batch in enumerate(image_batches)}
            for index, future in enumerate(as_completed(futures_to_request)):
                try:
                    process_workers(index, future, params)
                    total_bar()
                except Exception as e:
                    logger.error(f"{e}")
                    error_queue.put(futures_to_request[future])


def process_workers(worker_id: int, future: Future, params: DownloadCommandParameters) -> None:
    """
    Processes each image download worker.

    :param worker_id: The id of the worker.
    :param future: The future object to retrieve the response from.
    :param params: The CLI parameters.
    """
    # Wait for the result.
    res = future.result()
    # If response is empty, let the user know.
    if not res:
        logger.error(f"No data in response.")
        raise ValueError("Issue downloading images.")
    else:
        download_file = os.path.join(params.output, f"download_{worker_id}.zip")
        with open(download_file, "wb") as stream:
            for item in res:
                stream.write(item)


def get_image_ids(params: DownloadCommandParameters) -> List[str]:
    """
    Gather all the image ids for a specific Dataset.

    :param params: The command line parameters.
    :return: The ISIC image ids for the dataset requested.
    """
    if params.retry:
        logger.info(f"Attempting to download previously failed images.")
        with open(recovery_file_name()) as fh:
            image_ids = json.load(fh)
    else:
        df = pd.read_csv(params.metadata_file)
        image_ids = df[df["dataset"] == params.dataset]["isic_id"]

    return list(image_ids)


def chunks(data: list, n: int) -> list:
    """Yield successive n-sized chunks from data."""
    for i in range(0, len(data), n):
        yield data[i:i + n]


def make_request(api: IsicApi, image_set: list, params: DownloadCommandParameters) -> Union[List[dict], None]:
    """
    Make a image download request to the API.

    :param api: The reference to tha API object.
    :param image_set: The image name to retrieve data on.
    :param params: The command line parameters.
    :return: The data on the image or None.
    """
    # Convert to a json array
    url_image_ids = json.dumps(str(image_set))

    # Replace and switch quote notation for the API
    url_image_ids = url_image_ids.replace('"', "")
    url_image_ids = url_image_ids.replace("'", '"')
    # Quote all url strings.
    url_image_ids = urllib.parse.quote(url_image_ids)
    # Create the endpoint URL
    endpoint = f"image/download?include={params.include}&imageIds={url_image_ids}"

    # Request the images and return the response
    return api.get(endpoint=endpoint, timeout=params.timeout)
