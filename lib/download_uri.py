from logging import getLogger
import sys
import traceback
import requests

logger = getLogger(__name__)


def _fetch_uri(uri):
    try:
        uri = uri
        response = requests.get(uri)
        response.encoding = response.apparent_encoding
        response_csv = response.text
        return response_csv

    except:
        logger.error(traceback.format_exc())


def _save_to_local_disk(target_stirngs, dest_file_path):
    try:
        with open(dest_file_path, mode='a') as text_file:
            text_file.write(target_stirngs)
    except:
        logger.error(traceback.format_exc())


def download_uri(uri, dest_file_path):
    response = _fetch_uri(uri=uri)
    _save_to_local_disk(target_stirngs=response, dest_file_path=dest_file_path)