# coding=UTF-8
import urllib
import urllib.error
from html.parser import HTMLParser
from urllib import parse
from urllib.request import urlopen
import logging
from url_modify import asciify_url


class LinkParser(HTMLParser):
    def __init__(self, db_cursor):
        super().__init__()
        self.__baseUrl = ''
        self.__links = []
        self.__db_cursor = db_cursor

    def error(self, message):
        super(LinkParser, self).error(message)

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for (key, value) in attrs:
                if key == 'href':
                    new_url = parse.urljoin(self.__baseUrl, value)
                    self.__links.append(new_url)

    def parse(self, url):
        self.__baseUrl = url
        self.__links = []
        try:
            response = urlopen(asciify_url(url))
        except urllib.error.URLError as e:
            logging.warning(msg=e.reason)
            return

        if 'text/html' in response.getheader('Content-Type'):
            data = response.read()
            self.feed(data.decode('utf-8'))
            self.__db_cursor.add_data(url, data)

    def get_links(self):
        return self.__links


def spider(db_cursor):
    parser = LinkParser(db_cursor)
    pages = db_cursor.get_url()
    while pages:
        for page in pages:
            links = []
            parser.parse(page)
            links.extend(parser.get_links())
            db_cursor.add_url(links)
        pages = db_cursor.get_url()
