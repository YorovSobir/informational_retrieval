import requests
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
import urllib.parse
import logging
import time
import reppy
import re

headers = {'User-Agent': 'MedBot', 'Content-type': 'text/html'}
robots_agent = {}
base_links = set()
unknown_links = []


def get_urls(db_cursor, url):
    try:
        parsed_url = urllib.parse.urlparse(url)
    except ValueError as e:
        logging.warning(str(e))
        return []

    if parsed_url.netloc not in base_links:
        unknown_links.append(url)
        return []

    try:
        response = requests.get(url, headers=headers)
        time.sleep(0.1)
        http_encoding = response.encoding if 'charset' in response.headers.get('content-type', '').lower() else None
        html_encoding = EncodingDetector.find_declared_encoding(response.content, is_html=True)
        encoding = html_encoding or http_encoding
        soup = BeautifulSoup(response.content, "lxml", from_encoding=encoding)
        result = []
        db_cursor.add_data(url, soup.text.encode())
        for link in soup.find_all('a', href=True):
            result.append(urllib.parse.urljoin(url, link['href']))
        return result
    except requests.exceptions.ReadTimeout:
        logging.error("Read timeout")
    except requests.exceptions.ConnectTimeout:
        logging.error("Connect timeout")
    except:
        logging.error("Exception")


def spider(db_cursor):
    for link in db_cursor.get_base():
        base_links.add(link)
    pages = db_cursor.get_url(10)
    while pages:
        for page in pages:
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
                continue
            try:
                robots_url = reppy.Robots.robots_url(page)
                if page not in robots_agent:
                    robots = reppy.Robots.fetch(robots_url)
                    agent = robots.agent(headers["User-Agent"])
                    robots_agent[robots_url] = agent
                agent = robots_agent[robots_url]
                if not agent.allowed(page):
                    logging.info("Disallow crawling " + page)
                    continue
            except:
                logging.error("Parse Robot.txt " + page)
            links = []
            links.extend(get_urls(db_cursor, page))
            db_cursor.add_url(links)
        pages = db_cursor.get_url(10)
