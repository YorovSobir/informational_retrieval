import argparse
import re
import urllib.parse
import os
import logging
import pymorphy2
from nltk.corpus import stopwords


def build_parser():
    parser = argparse.ArgumentParser(add_help=False, description='Index for our information retrieval system')
    parser.add_argument('-h', '--host', default='localhost',
                        help='address of postgres server')
    parser.add_argument('-p', '--port', default='5432',
                        help='port of postgres server')
    parser.add_argument('-d', '--database', default='ir_db',
                        help='database to connect')
    parser.add_argument('-u', '--user', default='ir_med',
                        help='user in postgres server')
    parser.add_argument('--password', default='medicine',
                        help='password for user in postgres server')
    parser.add_argument('--index_dir', default='./index',
                        help='directory where we store index')
    parser.add_argument('--data_dir', default='./data',
                        help='directory where we store data')
    return parser


def url_to_path(url, data_dir):
    parsed_url = urllib.parse.urlparse(url)
    full_path = os.path.join(data_dir, parsed_url.netloc) + parsed_url.path
    full_path = re.sub(r'[<>|:&\s\\;().]', '_', full_path)
    return full_path


def id_and_path_to_doc(data_dir, db_service):
    return [(id_doc, url_to_path(url, data_dir))
            for id_doc, url in db_service.get_id_and_url()]


def store(data_dir, url, content):
    try:
        full_path = url_to_path(url, data_dir)
    except ValueError as e:
        logging.warning('exception while parsing url = ' + url + '; ' + str(e))
        return

    os.makedirs(full_path, exist_ok=True)
    with open(os.path.join(full_path, 'content.txt'), 'w') as f:
        f.write(content)


morph = pymorphy2.MorphAnalyzer()


def preprocess_text(data):
    words = re.sub(r'[^\u0410-\u044F\u0401\u0451\s]', ' ', data).split()
    return [morph.parse(word)[0].normal_form
            for word in words if word not in stopwords.words('russian')]
