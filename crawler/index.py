import re
from nltk.corpus import stopwords
import pymorphy2
from bs4 import BeautifulSoup
from collections import Counter
import pickle
import logging


class Index:
    def __init__(self, db_cursor, index_path):
        self.__db_cursor = db_cursor
        self.__index_path = index_path

    def create(self):
        data = []
        [data.append(self.__tokens(i, doc)) for i, doc in self.__next_document()]
        common_dict = {}
        for doc_id, words in data:
            for word in words.keys():
                if word not in common_dict:
                    common_dict[word] = []
                common_dict[word] += [(doc_id, words[word])]
        return common_dict

    def __next_document(self):
        data1 = 'Словари распространяются 13 отдельными пакетами 13'
        data2 = 'Словари отдельными пакетами 13'
        yield 1, data1
        yield 2, data2

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

    def serialize(self, dictionary, file_name):
        with open(file_name, 'wb') as f:
            try:
                pickle.dump(dictionary, f)
            except pickle.PicklingError as e:
                logging.error(str(e))

    def deserialize(self, file_name):
        with open(file_name, 'rb') as f:
            try:
                return pickle.load(f)
            except pickle.UnpicklingError as e:
                logging.error(str(e))


if __name__ == "__main__":
    a = Index(None, './index')
    pickle.dump(a.create(), open('index.ind', 'wb'))
