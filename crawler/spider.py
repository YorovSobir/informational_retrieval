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
        result = []
        try:
            parsed_url = urllib.parse.urlparse(url)
        except ValueError as e:
            logging.warning(str(e))
            return result

        if parsed_url.netloc not in self.__base_urls:
            self.__unknown_urls.append(url)
            return result

        try:
            response = requests.get(url, headers=self.__headers)
            if response.status_code != requests.codes.ok:
                logging.warning("Invalid status code " + response.status_code)
                return result
            time.sleep(0.1)
            http_encoding = response.encoding if 'charset' in response.headers.get('content-type', '').lower() else None
            html_encoding = EncodingDetector.find_declared_encoding(response.content, is_html=True)
            encoding = html_encoding or http_encoding
            soup = BeautifulSoup(response.content, "lxml", from_encoding=encoding)
            self.__db_cursor.add_data(url, soup.text.encode())
            for tag in soup.find_all('a', href=True):
                if tag is None:
                    logging.warning("invalid tag in link " + url)
                    continue
                result.append(urllib.parse.urljoin(url, tag['href']))
        except requests.exceptions.ReadTimeout:
            logging.error("Read timeout")
        except requests.exceptions.ConnectTimeout:
            logging.error("Connect timeout")
        except:
            logging.error("Exception")
        finally:
            return result

    def process_url(self, url):
        regex = re.compile(
            r'^(?:http|ftp)s?://'
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'
            r'localhost|'
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
            r'(?::\d+)?'
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        m = regex.match(url)
        if not m:
            logging.info("Invalid url " + url)
            return
        try:
            robots_url = reppy.Robots.robots_url(url)
            if url not in self.__robots_agent:
                robots = reppy.Robots.fetch(robots_url)
                agent = robots.agent(self.__headers["User-Agent"])
                self.__robots_agent[robots_url] = agent
            agent = self.__robots_agent[robots_url]
            if not agent.allowed(url):
                logging.info("Disallow crawling " + url)
                return
        except:
            logging.error("Parse Robot.txt " + url)
        urls = self.get_urls(url)
        self.__db_cursor.add_url(urls)

    def spider(self):
        for url in self.__db_cursor.get_base():
            self.__base_urls.add(url)
        urls = self.__db_cursor.get_url(self.__default)
        while urls:
            for url in urls:
                self.process_url(url)
            urls = self.__db_cursor.get_url(self.__default)
            if not urls and self.__unknown_urls:
                for url in self.__db_cursor.get_base():
                    self.__base_urls.add(url)
                urls = self.__unknown_urls
                self.__unknown_urls.clear()
