import re
from nltk.corpus import stopwords
import pymorphy2
from bs4 import BeautifulSoup
from collections import Counter
import pickle
import os
from pathlib import Path


class Index:
    def __init__(self, db_cursor, store):
        self.__db_cursor = db_cursor
        self.__store = store
        self.__common_dict = {}

    def create(self):
        data = []
        [data.append(self.__tokens(i, doc)) for i, doc in self.__next_document()]
        for doc_id, words in data:
            for word in words.keys():
                if word not in self.__common_dict:
                    self.__common_dict[word] = []
                self.__common_dict[word] += [(doc_id, words[word])]
        return self.__common_dict

    def save(self, path, file_name):
        pickle.dump(self.__common_dict, open(os.path.join(path, file_name), 'wb'))

    def load(self, path, file_name):
        self.__common_dict = pickle.load(open(os.path.join(path, file_name), 'rb'))

    def __next_document(self):
        cmd = 'SELECT id, url FROM storage'
        self.__db_cursor.execute(cmd)
        result = self.__db_cursor.fetchall()
        for idx, url in result:
            path = self.__store.url_to_path(url)
            yield idx, Path(os.path.join(path, 'content.txt')).read_text()

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
