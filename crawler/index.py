import re

import nltk
from nltk.corpus import stopwords
import pymorphy2
from bs4 import BeautifulSoup
from collections import Counter
import pickle
import logging
import os
from pathlib import Path


class Index:
    def __init__(self, db_service, store, index_path):
        self.__db_cursor = db_service.cur
        self.__store = store
        self.__common_dict = {}
        self.__index_path = index_path

    def create(self):
        data = [(self.__tokens(i, doc)) for i, doc in self.__next_document()]
        for doc_id, words in data:
            for word in words.keys():
                if word not in self.__common_dict:
                    self.__common_dict[word] = []
                self.__common_dict[word] += [(doc_id, words[word])]
        return self.__common_dict

    def __next_document(self):
        cmd = 'SELECT id, url FROM storage'
        self.__db_cursor.execute(cmd)
        result = self.__db_cursor.fetchall()
        for idx, url in result:
            fullpath = self.__store.url_to_path(url)
            path = Path(os.path.join(fullpath, 'content.txt'))
            if path.exists():
                print(idx)
                yield idx, path.read_text()

    def __tokens(self, i, raw_data):
        morph = pymorphy2.MorphAnalyzer()
        raw_data = BeautifulSoup(raw_data, 'lxml').text
        words = re.sub(r'[^А-я0-9ёЁ ]', '', raw_data).lower().split()
        words = [morph.parse(word)[0].normal_form for word in words]
        words = [word for word in words if word not in stopwords.words('russian')]
        words_to_count = Counter(words)
        result = {}
        for word in words_to_count.keys():
            result[word] = [idx for idx, x in enumerate(words) if x == word]
        return i, result

    def serialize(self, file_name):
        with open(os.path.join(self.__index_path, file_name), 'wb') as f:
            try:
                pickle.dump(self.__common_dict, f)
            except pickle.PicklingError as e:
                logging.error(str(e))

    def deserialize(self, file_name):
        with open(os.path.join(self.__index_path, file_name), 'rb') as f:
            try:
                self.__common_dict = pickle.load(f)
            except pickle.UnpicklingError as e:
                logging.error(str(e))
