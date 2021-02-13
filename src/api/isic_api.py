"""
Author:     David Walshe
Date:       12 February 2021

Code sourced from: https://raw.githubusercontent.com/ImageMarkup/isic-archive/master/scripts/isic_api.py
"""

import logging
import os
import requests

logger = logging.getLogger(__name__)


class IsicApi(object):

    def __init__(self,
                 hostname='https://isic-archive.com',
                 username=os.environ.get("ISIC_USERNAME", None),
                 password=os.environ.get("ISIC_PASSWORD", None)):

        self.base_url = f'{hostname}/api/v1'
        self.auth_token = None

        if username is not None:
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

        authToken = authResponse.json()['auth_token']['token']

        return authToken

    def get(self, endpoint):
        """
        Issues get request to ISIC storage service.

        :param endpoint: The endpoint to access.
        :return: The result of the GET request.
        """
        url = self._make_url(endpoint)
        print(url)
        headers = {'Girder-Token': self.auth_token} if self.auth_token else None
        return requests.get(url, headers=headers)

    def get_json(self, endpoint):
        """
        Returns the json content of the response.

        :param endpoint: The endpoint to access.
        :return: The JSON segment of the response.
        """
        return self.get(endpoint).json()

    def get_json_list(self, endpoint, limit=50, offset=0):
        """
        Retrieves a list of JSON objects depending on size of request.

        :param endpoint: The endpoint to access.
        :param limit: The limit of the responses.
        :param offset: The offset to start the request objects from.
        :return: A JSON response item.
        """
        # endpoint += '&' if '?' in endpoint else '?'

        while True:
            _endpoint = f'{endpoint}&limit={limit:d}&offset={offset:d}'

            resp = self.get(_endpoint).json()
            if not resp:
                break
            for elem in resp:
                yield elem
            offset += limit
