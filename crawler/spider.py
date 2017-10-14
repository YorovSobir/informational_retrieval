import requests
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
import urllib.parse
import logging
import time
import reppy
import re


class Spider:

    __headers = {'User-Agent': 'MedBot', 'Content-type': 'text/html'}
    __robots_agent = {}
    __base_urls = set()
    __unknown_urls = []
    __default = 10

    def __init__(self, db_cursor):
        self.__db_cursor = db_cursor

    def get_urls(self, url):
        try:
            parsed_url = urllib.parse.urlparse(url)
        except ValueError as e:
            logging.warning(str(e))
            return []

        if parsed_url.netloc not in self.__base_urls:
            self.__unknown_urls.append(url)
            return []

        try:
            response = requests.get(url, headers=self.__headers)
            time.sleep(0.1)
            http_encoding = response.encoding if 'charset' in response.headers.get('content-type', '').lower() else None
            html_encoding = EncodingDetector.find_declared_encoding(response.content, is_html=True)
            encoding = html_encoding or http_encoding
            soup = BeautifulSoup(response.content, "lxml", from_encoding=encoding)
            result = []
            self.__db_cursor.add_data(url, soup.text.encode())
            for link in soup.find_all('a', href=True):
                result.append(urllib.parse.urljoin(url, link['href']))
            return result
        except requests.exceptions.ReadTimeout:
            logging.error("Read timeout")
        except requests.exceptions.ConnectTimeout:
            logging.error("Connect timeout")
        except:
            logging.error("Exception")

    def process_url(self, page):
        regex = re.compile(
            r'^(?:http|ftp)s?://'
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'
            r'localhost|'
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
            r'(?::\d+)?'
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        m = regex.match(page)
        if not m:
            logging.info("Invalid url " + page)
            return
        try:
            robots_url = reppy.Robots.robots_url(page)
            if page not in self.__robots_agent:
                robots = reppy.Robots.fetch(robots_url)
                agent = robots.agent(self.__headers["User-Agent"])
                self.__robots_agent[robots_url] = agent
            agent = self.__robots_agent[robots_url]
            if not agent.allowed(page):
                logging.info("Disallow crawling " + page)
                return
        except:
            logging.error("Parse Robot.txt " + page)
        links = []
        links.extend(self.get_urls(page))
        self.__db_cursor.add_url(links)

    def spider(self):
        for link in self.__db_cursor.get_base():
            self.__base_urls.add(link)
        urls = self.__db_cursor.get_url(self.__default)
        while urls:
            for url in urls:
                self.process_url(url)
            urls = self.__db_cursor.get_url(self.__default)
            if not urls and self.__unknown_urls:
                for link in self.__db_cursor.get_base():
                    self.__base_urls.add(link)
                urls = self.__unknown_urls
                self.__unknown_urls.clear()
