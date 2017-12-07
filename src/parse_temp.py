import re
from pathlib import Path

import os
import psycopg2 as pg_driver
from bs4 import BeautifulSoup, Tag, NavigableString
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED

from utils.utils import url_to_path


def get_tag_text(tag):
    if tag.string:
        return tag.string
    return '\n'.join(get_tag_text(t) for t in tag)


def get_treatment(tag):
    result = ''
    for t in tag.next_siblings:
        if isinstance(t, Tag) and t.name == tag.name and not result:
            break
        result += get_tag_text(t)
    return result


def parse_treatment(tag):
    result = ''
    if isinstance(tag, Tag):
        for t in tag.contents:
            if isinstance(t, NavigableString) and ('лечение' in t.lower() and 'лечение в Санкт' not in t.lower()):
                result += get_treatment(tag)
            elif isinstance(t, Tag):
                for t2 in t.contents:
                    if (isinstance(t2, NavigableString) and 'лечение' in t2.lower()) or \
                            (t2.string and 'лечение' in t2.string.lower()):
                        result += get_treatment(tag)

    return result


def __parse_likar(html):
    soup = BeautifulSoup(html, "lxml")
    title = soup.find_all('h1', {"class": "article-title"})[0]
    # title = soup.head.title
    result = ''
    for tag in soup.find_all(name=['h2', 'h3', 'h4', 'p']):
        result += parse_treatment(tag)

    return title, result


def __parse_medicine(html):
    soup = BeautifulSoup(html, "lxml")
    title = soup.find_all('h1', {'itemprop': 'name'})[0]
    result = ''
    for tag in soup.find_all(name=['h2']):
        result += parse_treatment(tag)
    print(title)
    print(len(result))
    return title, result


def __parse_krasotaimedicina(html):
    soup = BeautifulSoup(html, "lxml")
    title = soup.find_all('h1')[0]
    result = ''
    for tag in soup.find_all(name=['h2']):
        result += parse_treatment(tag)
    print(title.text.strip())
    if len(result) < 20:

    print(len(result))
    return title, result


if __name__ == '__main__':
    db = pg_driver.connect(user='ir_med', password='medicine', host='192.168.1.215', dbname='ir_db')
    db.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
    cur = db.cursor()
    cur.execute('select url from storage where url like \'%zdorovieinfo%\'')
    res = cur.fetchall()
    count = 0
    for r in res:
        full_path = url_to_path(r[0], data_dir='/Users/vadim/informational_retrieval/src/data')
        path = Path(os.path.join(full_path, 'content.txt'))
        if path.exists():
            html = path.read_text(encoding='utf-8')
            try:
                if not __parse_krasotaimedicina(html):
                    print(r[0])
                else:
                    print(r[0])
            except:
                print(r[0])
                count += 1
        else:
            print('path not exists ' + r[0])
    print("All {0} \n Bad {1}".format(len(res), count))

