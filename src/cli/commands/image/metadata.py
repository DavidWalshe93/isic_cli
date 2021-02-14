"""
Author:     David Walshe
Date:       12 February 2021
"""

import os
import logging
import json
from time import sleep
from collections import namedtuple, OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Union, List, Tuple

import click
import pandas as pd
from alive_progress import alive_bar

from src.api.isic_api import IsicApi
from src.cli.config import COMMAND_CONTEXT_SETTINGS
from src.cli.utils import kwargs_to_namedtuple

logger = logging.getLogger(__name__)

MetadataCommandParameters = namedtuple("MetadataCommandParameters", ["input", "output", "retry", "timeout", "batch_delay", "batch_size", "workers"])


@click.command(**COMMAND_CONTEXT_SETTINGS, short_help="Download metadata for a list of images.")
@click.option("-i", "--input", type=str, required=True, help="A json file with a list of image names to download.")
@click.option("-o", "--output", type=str, default="metadata.csv", help="The name of the output file to save the metadata.")
@click.option("--retry", is_flag=True, help="Tries to download the missing records from previous attempts.")
@click.option("--timeout", type=int, default=5, help="The timeout length for each request to the API.")
@click.option("--batch-delay", type=int, default=5, help="Delay in seconds between each batch.")
@click.option("-b", "--batch-size", type=int, default=200, help="The request batch size.")
@click.option("-w", "--workers", type=int, default=5, help=f"Specify how many concurrent workers should be used. Default: 5")
@kwargs_to_namedtuple(MetadataCommandParameters)
def metadata(params: MetadataCommandParameters):
    """
    Command to download metadata for images using the ISIC Image API.
    """
    # Read the image names to request from the API.
    image_names = read_image_list(params=params)

    # Get the results and errors of the API requests
    results, errors = download_metadata(params=params, image_names=image_names)

    if errors:
        logger.error(f"{len(errors)} records were not downloaded. Use '--retry' to collect the missing records.")
        with open("missing_records.json", "w") as fh:
            json.dump(errors, fh, indent=4)

    # Convert the results to a pandas DataFrame object.
    df = process_results(params, results)

    # Save the results to disk.
    df.to_csv(params.output, index=True)


def read_image_list(params: MetadataCommandParameters) -> List[str]:
    """
    Reads the image names from a json file.

    :param params: The cli parameters.
    :return: The list of images to request
    """
    if params.retry:
        src_file = "./missing_records.json"
    else:
        src_file = params.input

    with open(src_file) as fh:
        return json.load(fh)


def download_metadata(params: MetadataCommandParameters, image_names: List[str]) -> Tuple[List[dict], List[str]]:
    """
    Uses a thread pool to download image metadata concurrently.

    :return: The tuple of metadata results and names of images that failed to download.
    """
    api = IsicApi()
    results = []
    errors = []

    # Measure the download progress for the full dataset.
    with alive_bar(len(image_names), title="Total Progress") as total_bar:
        for batch, chunk in enumerate(chunks(image_names, params.batch_size), start=1):

            # Wait 5 seconds between batches to prevent API throttling
            if batch > 1:
                sleep(params.batch_delay)

            # Run concurrent workers to download metadata.
            logger.info(f"Processing batch {batch} of {params.batch_size} requests.")
            with ThreadPoolExecutor(max_workers=params.workers) as executor:
                future_to_request = {executor.submit(make_request, api, image_name, params.timeout): image_name for image_name in chunk}

                for future in as_completed(future_to_request):
                    image_name = future_to_request[future]
                    try:
                        res = future.result()
                        results.append(res)
                        total_bar()
                    except Exception as e:
                        errors.append(image_name)

    return results, errors


def process_results(params: MetadataCommandParameters, results: List[dict]) -> pd.DataFrame:
    """
    Creates a pandas dataframe of the API results.

    :param params: The CLI parameters.
    :param results: The results from the API call.
    :return: A dataframe containing all the API results.
    """
    if params.retry and len(results) > 0:
        # Read in the existing data from disk.
        df = pd.read_csv(params.output, index_col="isic_id")
        # Create a dataframe from the retry data.
        df_retry = pd.DataFrame(results).set_index(["isic_id"])
        # Get all the new records currently not in the existing dataset.
        new_records = df_retry[~df_retry.index.isin(df.index)]

        # Concatenate all new records to the overall dataframe.
        df = pd.concat([df, new_records], axis=0)
    else:
        # If nto a retry attempt, then save all result data to a DataFrame.
        df = pd.DataFrame(results).set_index(["isic_id"])

    return df


def chunks(data: list, n: int) -> list:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(data), n):
        yield data[i:i + n]


def make_request(api: IsicApi, image_name: str, timeout: int) -> Union[dict, None]:
    """
    Make a metadata request to the API.

    :param api: The reference to tha API object.
    :param image_name: The image name to retrieve data on.
    :param timeout: Timeout in seconds.
    :return: The data on the image or None.
    """
    endpoint = f"image?" \
               f"detail=true"

    # Add the image name to the query.
    endpoint += f"&name={image_name}"

    res = api.get_json(endpoint=endpoint, timeout=timeout)

    if not res:
        logger.error(f"No data available for '{image_name}'")
    else:
        return process_metadata(res[0], image_name)


def process_metadata(data: dict, image_name: str) -> OrderedDict:
    """
    Processes the raw metadata of the API response into a CSV friendly format.

    :return: A dictionary with all relative information on the image.
    """
    return OrderedDict(
        isic_id=data["_id"],
        image_name=image_name,
        dataset=data["dataset"]["name"],
        pixels_x=data["meta"]["acquisition"]["pixelsX"],
        pixels_y=data["meta"]["acquisition"]["pixelsY"],
        age=data["meta"]["clinical"]["age_approx"],
        sex=data["meta"]["clinical"]["sex"],
        localization=data["meta"]["clinical"]["anatom_site_general"],
        benign_malignant=data["meta"]["clinical"]["benign_malignant"],
        dx=data["meta"]["clinical"]["diagnosis"],
        dx_type=data["meta"]["clinical"]["diagnosis_confirm_type"],
        melanocytic=data["meta"]["clinical"]["melanocytic"],
    )
