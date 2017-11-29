import re
import urllib.parse
import logging
import os


def url_to_path(url, data_path):
    try:
        parsed_url = urllib.parse.urlparse(url)
    except ValueError as e:
        logging.warning(str(e))
        return ''
    full_path = os.path.join(data_path, parsed_url.netloc) + parsed_url.path
    full_path = re.sub(r'[<>|:&\s\\;()]', '', full_path)
    return full_path


def id_and_path_to_doc(data_dir, db_service):
    return [(id_doc, url_to_path(url, data_dir))
            for id_doc, url in db_service.get_id_and_url()]
