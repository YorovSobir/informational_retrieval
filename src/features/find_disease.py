from bs4 import BeautifulSoup, Tag, NavigableString
from pathlib import Path
import os
from DBConnect import DBService
from store import Store


class Parser:
    def __init__(self, base='ru.likar.info/bolezni/'):
        self.__base = base

    def parse(self, html):
        if self.__base == 'ru.likar.info/bolezni/':
            return self.__parse_likar(html)
        elif self.__base == 'www.diagnos.ru/diseases/':
            return self.__parse_diagnos(html)
        elif self.__base == 'www.genesha.ru/diseases/':
            return self.__parse_genesha(html)

    def get_tag_text(self, tag):
        if tag.string:
            return tag.string
        return '\n'.join(self.get_tag_text(t) for t in tag)

    def get_treatment(self, tag):
        result = ''
        for t in tag.next_siblings:
            if isinstance(t, Tag) and t.name == tag.name and not result:
                break
            result += self.get_tag_text(t)
        return result

    def parse_treatment(self, tag):
        result = ''
        if isinstance(tag, Tag):
            for t in tag.contents:
                if isinstance(t, NavigableString) and 'лечение' in t.lower():
                    result += self.get_treatment(tag)
                elif isinstance(t, Tag):
                    for t2 in t.contents:
                        if (isinstance(t2, NavigableString) and 'лечение' in t2.lower()) or \
                                (t2.string and 'лечение' in t2.string.lower()):
                            result += self.get_treatment(tag)

        return result

    def __parse_likar(self, html):
        soup = BeautifulSoup(html, "lxml")
        title = soup.find_all('h1', {"class": "article-title"})[0]
        # title = soup.head.title
        result = ''
        for tag in soup.find_all(name=['h2', 'h3', 'h4', 'p']):
            result += self.parse_treatment(tag)

        return title, result

    def __parse_diagnos(self, html):
        soup = BeautifulSoup(html, "lxml")
        title = soup.find_all('h1', {'itemprop': 'name'})[0]
        result = ''
        for h2 in soup.find_all('h2'):
            if 'лечение' in h2.next.lower():
                for tag in h2.next_siblings:
                    if tag.name == 'h2':
                        break
                    elif tag == '\n':
                        result += tag
                    else:
                        result += tag.text
        return title.next, result

    def __parse_genesha(self, html):
        soup = BeautifulSoup(html, "lxml")
        title = soup.find_all('h1', {'class': 'b-heading__title'})[0]
        return title.next, None


class FindDisease:
    def __init__(self, db_service, store):
        self.__cur = db_service.cur
        self.__store = store
        self.__db = db_service.db
        self.__base = ['ru.likar.info/bolezni/', 'www.diagnos.ru/diseases/', 'www.genesha.ru/diseases/']

    def find(self):
        for base in self.__base:
            parser = Parser(base)
            cmd = "SELECT id, url FROM storage WHERE url LIKE '%{0}%'".format(base)
            self.__cur.execute(cmd)
            result = self.__cur.fetchall()
            for idx, url in result:
                full_path = self.__store.url_to_path(url)
                path = Path(os.path.join(full_path, 'content.txt'))
                if path.exists():
                    html = path.read_text(encoding='utf-8')
                    title, diseaseHelp = parser.parse(html)
                    if title is None:
                        continue
                    cmd = 'INSERT INTO disease_treatment VALUES({0}, \'{1}\', \'{2}\')'.format(idx, title, diseaseHelp)
                    self.__cur.execute(cmd)
                    self.__db.commit()


if __name__ == '__main__':
    db_service = DBService(user='ir_med', password='medicine', host='localhost', dbname='ir_db')
    store = Store('./data')
    FD = FindDisease(db_service, store)
    FD.find()
