import os
import pymorphy2
from bs4 import BeautifulSoup
from multiprocessing import Pool
import multiprocessing
from pathlib import Path
import re
from nltk.corpus import stopwords
import pickle
import logging
from utils.utils import url_to_path, id_and_path_to_doc


class Data:
    def __init__(self, db_service, data_dir):
        self.__db_service = db_service
        self.__data_dir = data_dir

    def store(self, url, content):
        full_path = url_to_path(self.__data_dir, url)
        if full_path == '':
            return
        os.makedirs(full_path, exist_ok=True)
        with open(os.path.join(full_path, 'content.txt'), 'w') as f:
            f.write(content)

    def preprocess(self):
        id_and_path = id_and_path_to_doc(self.__data_dir, self.__db_service)
        with Pool(multiprocessing.cpu_count()) as p:
            while id_and_path:
                p.map(clean_doc, id_and_path)
                id_and_path = id_and_path_to_doc(self.__data_dir, self.__db_service)


morph = pymorphy2.MorphAnalyzer()


def clean_doc(document):
    idx, full_path = document
    path = Path(os.path.join(full_path, 'content.txt'))
    if path.exists():
        raw_data = path.read_text()
        raw_data = BeautifulSoup(raw_data, 'lxml').getText()
        words = re.sub(r'[^А-я0-9ёЁ ]', '', raw_data).split()
        words = [morph.parse(word)[0].normal_form for word in words if word not in stopwords.words('russian')]
        with open(os.path.join(full_path, 'words'), 'wb') as f:
            try:
                pickle.dump(words, f)
            except pickle.PicklingError as e:
                logging.error(str(e))
    else:
        logging.warning("file not found: " + os.path.join(full_path, 'content.txt'))
