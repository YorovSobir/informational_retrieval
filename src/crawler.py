import requests
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
import urllib.parse
import logging
import time
import reppy
import re

from utils.db_service import DBService
from utils.utils import store, build_parser


class Crawler:

    __headers = {'User-Agent': 'MedBot', 'Content-type': 'text/html'}
    __robots_agent = {}
    __base_urls = []
    __default = 10

    def __init__(self, db_service, data_dir):
        self.__db_service = db_service
        self.__data_dir = data_dir

    def get_urls(self, url, delay):
        time.sleep(delay)
        result = []
        # try:
        #     parsed_url = urllib.parse.urlparse(url)
        # except ValueError as e:
        #     logging.warning(str(e))
        #     return result
        #
        # if parsed_url.netloc not in self.__base_urls:
        #     logging.info('url = ' + parsed_url.netloc + ' are not in base')
        #     return result
        from_based_urls = False
        for base_url in self.__base_urls:
            if base_url in url:
                from_based_urls = True
        if not from_based_urls:
            logging.warning('url = ' + url + ' are not from base')
            return result
        try:
            response = requests.get(url, headers=self.__headers)
            if response.status_code != requests.codes.ok:
                logging.warning("Invalid status code " + response.status_code)
                return result
            http_encoding = response.encoding if 'charset' in response.headers.get('content-type', '').lower() else None
            html_encoding = EncodingDetector.find_declared_encoding(response.content, is_html=True)
            encoding = html_encoding or http_encoding
            soup = BeautifulSoup(response.content, "lxml", from_encoding=encoding)
            try:
                store(self.__data_dir, url, response.content.decode(encoding))
                self.__db_service.add_data(url)
            except FileNotFoundError as e:
                logging.warning('url = ' + url + ' ' + str(e))

            for tag in soup.find_all('a', href=True):
                if tag is None:
                    logging.warning("invalid tag in link " + url)
                    continue
                result.append(urllib.parse.urljoin(url, tag['href']))
            self.__db_service.update_incoming_links(result)
        except requests.exceptions.ReadTimeout:
            logging.error("Read timeout")
        except requests.exceptions.ConnectTimeout:
            logging.error("Connect timeout")
        except:
            logging.error("Exception while parsed url = " + url)
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
        agent = None
        try:
            robots_url = reppy.Robots.robots_url(url)
            if robots_url not in self.__robots_agent:
                robots = reppy.Robots.fetch(robots_url)
                agent = robots.agent(self.__headers["User-Agent"])
                self.__robots_agent[robots_url] = agent
            agent = self.__robots_agent[robots_url]
            if not agent.allowed(url):
                logging.info("Disallow crawling " + url)
                return
        except:
            logging.error("Parse Robot.txt " + url)
        if agent is None or agent.delay is None:
            delay = 0.5
        else:
            delay = agent.delay

        urls = self.get_urls(url, delay)
        self.__db_service.add_url(urls)

    def run(self):
        self.__base_urls = [url for url in self.__db_service.get_base()]
        urls = self.__db_service.get_url(self.__default)
        while urls:
            for url in urls:
                self.process_url(url)
            urls = self.__db_service.get_url(self.__default)


def crawler(db, urls, data_dir):
    # urls_domain = []
    # for url in urls:
    #     try:
    #         domain = urllib.parse.urlparse(url)
    #     except ValueError as e:
    #         logging.warning(str(e))
    #         continue
    #     urls_domain.append(domain.netloc)
    db.add_base(urls)
    db.add_url(urls)
    Crawler(db, data_dir).run()


if __name__ == '__main__':
    parser = build_parser()
    args = parser.parse_args()
    db = DBService(user=args.user, password=args.password, host=args.host, dbname=args.database)
    crawler(db, args.urls, args.data_dir)

