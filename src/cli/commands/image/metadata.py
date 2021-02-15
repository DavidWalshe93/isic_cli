"""
Author:     David Walshe
Date:       12 February 2021
"""

import os
import logging
import json
import pprint
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

MetadataCommandParameters = namedtuple("MetadataCommandParameters", ["output", "retry", "timeout", "limit", "offset", "batch_size", "workers"])


@click.command(**COMMAND_CONTEXT_SETTINGS, short_help="Download metadata for a list of images.")
@click.option("--limit", type=int, default=500, help="Result set size limit. Default=500")
@click.option("--batch-size", type=int, default=100, help="The request batch size. Default=100")
@click.option("--offset", type=int, default=0, help="Offset into result set. Default=0")
@click.option("-o", "--output", type=str, default="metadata.csv", help="The name of the output file to save the metadata.")
@click.option("--retry", is_flag=True, help="Tries to download the missing records from a previous attempt.")
@click.option("--timeout", type=int, default=5, help="The timeout length for each request to the API. Default=5")
@click.option("-w", "--workers", type=int, default=5, help=f"Specify how many concurrent workers should be used. Default=5")
@kwargs_to_namedtuple(MetadataCommandParameters)
def metadata(params: MetadataCommandParameters):
    """
    Command to download metadata for images using the ISIC Image API.
    """
    # Get the results and errors of the API requests
    results, errors = download_metadata(params=params)

    if errors:
        logger.error(f"{len(errors)} batches were not downloaded. Use '--retry' to collect the missing records.")
        with open("missing_records.json", "w") as fh:
            json.dump(errors, fh, indent=4)

    # Convert the results to a pandas DataFrame object.
    df = process_results(params, results)

    if df is not None:
        # Save the results to disk.
        df.to_csv(params.output, index=True)


def get_offsets(params: MetadataCommandParameters) -> List[int]:
    """
    Gets the offset ranges for the requests.

    :param params: The cli parameters.
    :return: The list of offset numbers.
    """
    if params.retry:
        src_file = "./missing_records.json"

        with open(src_file) as fh:
            return json.load(fh)
    else:
        return [i for i in range(params.offset, params.limit, params.batch_size)]


def download_metadata(params: MetadataCommandParameters) -> Tuple[List[dict], List[str]]:
    """
    Uses a thread pool to download image metadata concurrently.

    :return: The tuple of metadata results and names of images that failed to download.
    """
    api = IsicApi()
    results = []
    errors = []

    offsets = get_offsets(params=params)

    # Measure the download progress for the full dataset.
    with alive_bar(len(offsets), title="Total Progress", enrich_print=False) as total_bar:
        # Run concurrent workers to download metadata.
        with ThreadPoolExecutor(max_workers=params.workers) as executor:
            future_to_request = {executor.submit(make_request, api, params.batch_size, offset, params.timeout): offset for offset in offsets}

            for future in as_completed(future_to_request):
                offset = future_to_request[future]
                try:
                    res = future.result()
                    if not res:
                        logger.error(f"No data in response.")
                    else:
                        results = [*results, *res]
                    total_bar()
                except Exception as e:
                    logger.info(f"{e}")
                    errors.append(offset)

    return results, errors


def process_results(params: MetadataCommandParameters, results: List[dict]) -> Union[pd.DataFrame, None]:
    """
    Creates a pandas dataframe of the API results.

    :param params: The CLI parameters.
    :param results: The results from the API call.
    :return: A dataframe containing all the API results.
    """
    if not results:
        return None

    try:
        if params.retry:
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

        # Drop duplicate items based on their isic_id value.
        df = df[~df.index.duplicated(keep='first')]

        return df
    except Exception as e:
        pprint.pprint(results)
        raise e


def chunks(data: list, n: int) -> list:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(data), n):
        yield data[i:i + n]


def make_request(api: IsicApi, limit: int, offset: int, timeout: int) -> Union[List[dict], None]:
    """
    Make a metadata request to the API.

    :param api: The reference to tha API object.
    :param limit: The image name to retrieve data on.
    :param offset: The image name to retrieve data on.
    :param timeout: Timeout in seconds.
    :return: The data on the image or None.
    """
    endpoint = f"image?" \
               f"detail=true"

    res = api.get_json(endpoint=endpoint, limit=limit, offset=offset, timeout=timeout)

    if not res:
        logger.error(f"No data available.")
    else:
        return [process_metadata(item) for item in res]


def process_metadata(data: dict) -> OrderedDict:
    """
    Processes the raw metadata of the API response into a CSV friendly format.

    :return: A dictionary with all relative information on the image.
    """
    try:
        return OrderedDict(
            isic_id=data["_id"],
            image_name=data["name"],
            dataset=data["dataset"]["name"],
            description=data["dataset"]["description"],
            accepted=data["notes"]["reviewed"]["accepted"],
            created=data["created"].split("T")[0],
            tags=data["notes"]["tags"],
            pixels_x=data["meta"]["acquisition"]["pixelsX"],
            pixels_y=data["meta"]["acquisition"]["pixelsY"],
            age=data["meta"]["clinical"].get("age_approx", None),
            sex=data["meta"]["clinical"].get("sex", None),
            localization=data["meta"]["clinical"].get("anatom_site_general", None),
            benign_malignant=data["meta"]["clinical"].get("benign_malignant", None),
            dx=data["meta"]["clinical"].get("diagnosis", None),
            dx_type=data["meta"]["clinical"].get("diagnosis_confirm_type", None),
            melanocytic=data["meta"]["clinical"].get("melanocytic", None),
        )
    except Exception as e:
        pprint.pprint(data)
        raise e
