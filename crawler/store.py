import os
import re
import urllib.parse
import logging


class Store:
    def __init__(self, root_path):
        self.__root_path = root_path

    def store(self, url, content):
        try:
            full_path = self.url_to_path(url)
        except ValueError as e:
            logging.warning('error in url to path ' + str(e))
            return
        os.makedirs(full_path, exist_ok=True)
        with open(os.path.join(full_path, 'content.txt'), 'w') as f:
            f.write(content)

    def url_to_path(self, url):
        parsed_url = urllib.parse.urlparse(url)
        full_path = os.path.join(self.__root_path, parsed_url.netloc) + parsed_url.path
        full_path = re.sub(r'[<>|:&\s\\;()]', '', full_path)
        return full_path
