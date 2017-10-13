import urllib
import urllib.error
from html.parser import HTMLParser
from urllib import parse
from urllib.request import urlopen
import logging
from url_modify import asciify_url
import time
import reppy


class LinkParser(HTMLParser):
    def __init__(self, db_cursor):
        super().__init__()
        self.__baseUrl = ''
        self.__links = []
        self.__db_cursor = db_cursor
        # self.__robot_parser = RobotFileParser()
        # self.__regex_base_url = re.compile("((https?://)?.*?)/.*")
        self.__agent_name = "MedBot"
        self.__robots_agent = {}

    def error(self, message):
        super(LinkParser, self).error(message)

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for (key, value) in attrs:
                if key == 'href':
                    new_url = parse.urljoin(self.__baseUrl, value)
                    self.__links.append(new_url)

    def parse(self, url):
        robots_url = reppy.Robots.robots_url(url)
        if url not in self.__robots_agent:
            robots = reppy.Robots.fetch(robots_url)
            agent = robots.agent(self.__agent_name)
            self.__robots_agent[robots_url] = agent
        agent = self.__robots_agent[robots_url]

        if not agent.allowed(url):
            logging.info("Disallow crawling " + url)
            return
        self.__baseUrl = url
        self.__links = []
        time.sleep(1)
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
