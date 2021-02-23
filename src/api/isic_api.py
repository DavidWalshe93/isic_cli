"""
Author:     David Walshe
Date:       12 February 2021

Code sourced from: https://raw.githubusercontent.com/ImageMarkup/isic-archive/master/scripts/isic_api.py
"""

import logging
import os
import requests
from functools import wraps
from time import perf_counter

logger = logging.getLogger(__name__)


def timeit(func: callable):
    """
    Times how long a API request takes.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        url = args[1]

        if len(url) > 100:
            url = url[:100]
            url = f"{url}..."

        logger.info(f"Request: '{url}'")

        start_time = perf_counter()
        res = func(*args, **kwargs)
        end_time = perf_counter()

        logger.info(f"Response: '{url}' ({end_time - start_time:.2f}s).")

        return res

    return wrapper


class IsicApi(object):

    def __init__(self,
                 hostname='https://isic-archive.com',
                 login: bool = False,
                 username=os.environ.get("ISIC_USERNAME", None),
                 password=os.environ.get("ISIC_PASSWORD", None)):

        self.base_url = f'{hostname}/api/v1'
        self.auth_token = None

        if username is not None and login is True:
            if password is None:
                password = input(f'Password for user "{username}":')
            self.auth_token = self._login(username, password)
            logger.info(f"API token acquired.")
        else:
            logger.info(f"No login credentials found, sending request anonymously.")

    def _make_url(self, endpoint):
        """
        Helper to make the request url endpoint

        :param endpoint: The endpoint to access.
        :return: The base URL + the endpoint
        """
        return f'{self.base_url}/{endpoint}'

    def _login(self, username, password) -> str:
        """
        Attempts to login using passed username and passowrd

        :param username: ISIC username.
        :param password: ISIC password.
        :return: The bearer token to use for subsequent requests.
        :raises Exception: When login attempt fails.
        """
        authResponse = requests.get(
            self._make_url('user/authentication'),
            auth=(username, password)
        )
        if not authResponse.ok:
            raise Exception(f'Login error: {authResponse.json()["message"]}')

        authToken = authResponse.json()['authToken']['token']

        return authToken

    def get(self, endpoint, timeout: int = 5):
        """
        Issues get request to ISIC storage service.

        :param endpoint: The endpoint to access.
        :param timeout: The request timeout length in seconds.
        :return: The result of the GET request.
        """
        url = self._make_url(endpoint)
        headers = {'Girder-Token': self.auth_token} if self.auth_token else None

        return self._get(url, headers=headers, timeout=timeout)

    @timeit
    def _get(self, url: str, headers: dict, timeout: int):
        return requests.get(url, headers=headers, timeout=timeout)

    def get_json(self, endpoint, limit: int = 50, offset: int = 0, timeout: int = 5):
        """
        Returns the json content of the response.

        :param endpoint: The endpoint to access.
        :param limit: The limit of the responses.
        :param offset: The offset to start the request objects from.
        :param timeout: The request timeout length in seconds.
        :return: The JSON segment of the response.
        """
        _endpoint = f'{endpoint}&limit={limit:d}&offset={offset:d}'

        return self.get(_endpoint, timeout=timeout).json()

    def get_json_list(self, endpoint, limit=50, offset=0, timeout: int = 5):
        """
        Retrieves a list of JSON objects depending on size of request.

        :param endpoint: The endpoint to access.
        :param limit: The limit of the responses.
        :param offset: The offset to start the request objects from.
        :param timeout: The request timeout length in seconds.
        :return: A JSON response item.
        """
        # endpoint += '&' if '?' in endpoint else '?'

        while True:
            _endpoint = f'{endpoint}&limit={limit:d}&offset={offset:d}'

            resp = self.get(_endpoint, timeout=timeout).json()
            if not resp:
                break
            for elem in resp:
                yield elem
            offset += limit
