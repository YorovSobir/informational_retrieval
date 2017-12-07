from bs4 import BeautifulSoup, Tag, NavigableString
from pathlib import Path
import os

from utils.db_service import DBService
from utils.utils import url_to_path


class Parser:
    def __init__(self, base='ru.likar.info/bolezni/'):
        self.__base = base

    def parse(self, html):
        if self.__base == 'http://www.likar.info/bolezni/':
            return self.__parse_likar(html)
        elif self.__base == 'http://www.diagnos.ru/diseases/':
            return self.__parse_diagnos(html)
        elif self.__base == 'https://online-diagnos.ru/illness':
            return self.__parse_online_diagnos(html)
        elif self.__base == 'https://medaboutme.ru/zdorove/spravochnik/bolezni/':
            return self.__parse_medaboutme(html)
        elif self.__base == 'http://www.krasotaimedicina.ru/diseases/':
            return self.__parse_krasota_i_medicina(html)
        return None, None

    def get_tag_text(self, tag):
        if tag.string:
            return tag.string
        return '\n'.join(self.get_tag_text(t) for t in tag)

    def get_treatment(self, tag, break_tag):
        result = ''
        for t in tag.next_siblings:
            if isinstance(t, Tag) and t.name == break_tag and not result:
                break
            result += self.get_tag_text(t)
        return result

    def parse_treatment(self, tag):
        result = ''
        if isinstance(tag, Tag):
            for t in tag.contents:
                if isinstance(t, NavigableString) and 'лечение' in t.lower():
                    result += self.get_treatment(tag, tag.name)
                elif isinstance(t, Tag):
                    for t2 in t.contents:
                        if (isinstance(t2, NavigableString) and 'лечение' in t2.lower()) or \
                                (t2.string and 'лечение' in t2.string.lower()):
                            result += self.get_treatment(tag, tag.name)
        return result

    def __parse_likar(self, html):
        soup = BeautifulSoup(html, "lxml")
        title = soup.find('h1', {'class': 'article-title'})
        result = ''
        for tag in soup.find_all(name=['h2', 'h3', 'h4', 'p']):
            result += self.parse_treatment(tag)
        return title.text.strip(), result

    def __parse_online_diagnos(self, html):
        soup = BeautifulSoup(html, "lxml")
        title = soup.find('h1', {'class': 'title-h1'})
        if not title:
            return None, None
        result = ''
        for tag in soup.find_all(name=['h2']):
            if tag.string and 'лечение' in tag.string.lower():
                result += self.get_treatment(tag, 'h3')
        return title.text.strip(), result

    def __parse_medaboutme(self, html):
        soup = BeautifulSoup(html, "lxml")
        title = soup.find('h1')
        if not title:
            return None, None
        result = ''
        for tag in soup.findAll("div", {"class": "disease-detail-body"}):
            for t in tag.contents:
                if t.name == 'h3' and 'лечение' in t.string.lower():
                    result += self.get_tag_text(tag)
                    break
        return title.text.strip(), result

    def __parse_diagnos(self, html):
        soup = BeautifulSoup(html, "lxml")
        title = soup.find('h1', {'itemprop': 'name'})
        if not title:
            return None, None
        result = ''
        for tag in soup.find_all(name=['h2']):
            result += self.parse_treatment(tag)
        return title.text.strip(), result

    def __parse_krasota_i_medicina(self, html):
        soup = BeautifulSoup(html, "lxml")
        title = soup.find('h1')
        if not title:
            return None, None
        result = ''
        for tag in soup.find_all(name=['h2']):
            result += self.parse_treatment(tag)
        if len(result) < 20:
            return None, None
        return title.text.strip(), result


class FindDisease:
    def __init__(self, db_service, data_dir):
        self.__cur = db_service.db.cursor()
        self.__data_dir = data_dir
        self.__db = db_service.db
        self.__base = db_service.get_base()

    def find(self):
        for base in self.__base:
            parser = Parser(base)
            cmd = "SELECT id, url FROM storage WHERE url LIKE '%{0}%'".format(base)
            self.__cur.execute(cmd)
            result = self.__cur.fetchall()
            for idx, url in result:
                full_path = url_to_path(url, self.__data_dir)
                path = Path(os.path.join(full_path, 'content.txt'))
                if path.exists():
                    html = path.read_text(encoding='utf-8')
                    title, treatment = parser.parse(html)
                    if title is None or treatment is None:
                        continue
                    cmd = "INSERT INTO disease (val) VALUES (\'{0}\') " \
                          "ON CONFLICT(\"val\") DO UPDATE SET val = EXCLUDED.val RETURNING id".format(title)
                    self.__cur.execute(cmd)
                    id = self.__cur.fetchone()[0]
                    cmd = """
                        INSERT INTO doc_disease_treatment VALUES(%s, %s, %s)
                    """
                    self.__cur.execute(cmd, (idx, id, treatment))
                    self.__db.commit()


if __name__ == '__main__':
    db_service = DBService(user='ir_med', password='medicine', host='localhost', dbname='ir_db')
    data_dir = '/home/sobir/spbau/secondyear/temp/data'
    FD = FindDisease(db_service, data_dir)
    FD.find()
