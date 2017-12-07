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


def get_treatment(tag, break_tag='h3'):
    result = ''
    for t in tag.next_siblings:
        if isinstance(t, Tag) and t.name == break_tag and not result:
            break
        result += get_tag_text(t)
    return result


def parse_treatment(tag):
    result = ''
    if isinstance(tag, Tag):
        for t in tag.contents:
            if isinstance(t, NavigableString) and 'лечение' in t.lower():
                result += get_treatment(tag)
            elif isinstance(t, Tag):
                for t2 in t.contents:
                    if (isinstance(t2, NavigableString) and 'лечение' in t2.lower()) or \
                            (t2.string and 'лечение' in t2.string.lower()):
                        result += get_treatment(tag)

    return result


def __parse_online_diagnos(html):
    soup = BeautifulSoup(html, "lxml")
    title = soup.head.title
    result = ''
    for tag in soup.find_all(name=['h2']):
        if tag.string and 'лечение' in tag.string.lower():
            result += get_treatment(tag)
    if not result:
        return False
    return True
    # return title, result


def __parse_medaboutme(html):
    soup = BeautifulSoup(html, "lxml")
    title = soup.head.title
    result = ''
    for tag in soup.findAll("div", {"class": "disease-detail-body"}):
        for t in tag.contents:
            if t.name == 'h3' and 'лечение' in t.string.lower():
                result += get_tag_text(tag)
                break
    return title, result


if __name__ == '__main__':
    db = pg_driver.connect(user='ir_med', password='medicine', host='localhost', dbname='ir_db')
    db.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
    cur = db.cursor()
    cur.execute('select url from storage where url like \'%medaboutme%\'')
    res = cur.fetchall()
    first = False
    for r in res:
        full_path = url_to_path(r[0], data_dir='/home/sobir/spbau/secondyear/temp/data')
        path = Path(os.path.join(full_path, 'content.txt'))
        if path.exists():
            html = path.read_text(encoding='utf-8')
            if not __parse_medaboutme(html):
                print(r[0])
        else:
            print('path not exists ' + r[0])
            break
