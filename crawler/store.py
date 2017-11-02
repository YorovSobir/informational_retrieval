import os
import re
import urllib.parse
import logging


class Store:
    def __init__(self, root_path):
        self.__root_path = root_path

    def store(self, url, content):
        fullpath = self.url_to_path(url)
        if fullpath == '':
            return
        os.makedirs(fullpath, exist_ok=True)
        with open(os.path.join(fullpath, 'content.txt'), 'w') as f:
            f.write(content)

    def url_to_path(self, url):
        try:
            parsed_url = urllib.parse.urlparse(url)
        except ValueError as e:
            logging.warning(str(e))
            return ''
        fullpath = os.path.join(self.__root_path, parsed_url.netloc) + parsed_url.path
        fullpath = re.sub(r'[<>|:&\s\\;()]', '', fullpath)
        return fullpath
