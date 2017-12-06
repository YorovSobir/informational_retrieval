from bs4 import BeautifulSoup
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
        if len(tag.contents) == 1:
            return tag.string
        return '\n'.join(self.get_tag_text(t) for t in tag)

    def parse_tag(self, tag, tags):
        result = ''
        for t in tags:
                if 'лечение' in t.string.lower():
                    for t_in in t.next_siblings:
                        if t_in.name == tag:
                            break
                        elif t_in.name == 'p':
                            result += '\n' + self.get_tag_text(t_in)
                        elif t_in != '\n':
                            break

    def __parse_likar(self, html):
        soup = BeautifulSoup(html, "lxml")
        title = soup.find_all('h1', {"class": "article-title"})[0]
        result = ''
        for h2 in soup.find_all('h2'):
            try:
                if 'лечение' in h2.text.lower():
                    for tag in h2.next_siblings:
                        if tag.name == 'h2':
                            break
                        elif tag.name == 'p':
                            result += tag.text
                        elif tag != '\n':
                            break
            except:
                print(h2)
        if result == '':
            for h3 in soup.find_all('h3'):
                try:
                    if 'лечение' in h3.text.lower():
                        for tag in h3.next_siblings:
                            if tag.name == 'h2':
                                break
                            elif tag.name == 'p':
                                result += tag.text
                            elif tag != '\n':
                                break
                except:
                    print(h3)
        if result == '':
            for h3 in soup.find_all('p'):
                try:
                    if 'лечение' == h3.next.text.lower():
                        for tag in h3.next_siblings:
                            if tag.name == 'p' and h3.text.lower() != 'осложнения':
                                result += tag.text
                            elif tag != '\n':
                                break
                except:
                    pass
        if result == '':
            for h3 in soup.find_all('p'):
                try:
                    if 'лечение' in h3.next.text.lower():
                        for tag in h3.next_siblings:
                            if tag.name == 'p' and h3.text.lower() != 'осложнения':
                                result += tag.text
                            elif tag != '\n':
                                break
                except:
                    pass
        if result == '':
            for h3 in soup.find_all('p'):
                try:
                    if 'лечение' in h3.next.text.lower():
                        for tag in h3.next_siblings:
                            if tag.name == 'ul':
                                result += tag.text
                            elif tag != '\n' or h3.text.lower() == 'осложнения':
                                break
                except:
                    pass
        result = result.replace("'", "\"")
        return title.next, result

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
