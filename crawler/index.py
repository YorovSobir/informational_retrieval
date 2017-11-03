import re
import pymorphy2
from nltk.corpus import stopwords
from bs4 import BeautifulSoup
import pickle
import logging
import os
from pathlib import Path
from multiprocessing import Pool

morph = pymorphy2.MorphAnalyzer()


class Index:
    def __init__(self, db_service, store, index_path):
        self.__db_cursor = db_service.cur
        self.__store = store
        self.__common_dict = {}
        self.__index_path = index_path

    def create_for_first_latter(self):
        p = Pool(4)
        letters = [chr(i) for i in range(ord('а'), ord('я') + 1)]
        letters.extend([chr(i + 48) for i in range(10)])
        letters.append('ё')
        self.__common_dict = {key: dict() for key in letters}
        input = self.__get_documents()
        p.map(pre_process_letter, [(input, ch, self.__common_dict[ch]) for ch in letters])
        return self.__common_dict

    def create(self):
        p = Pool(4)
        input = self.__get_documents()
        data = filter(None.__ne__, p.map(pre_process, input))
        for doc_id, words in data:
            for word in words.keys():
                if word not in self.__common_dict:
                    self.__common_dict[word] = []
                self.__common_dict[word].append((doc_id, words[word]))
        return self.__common_dict

    def __get_documents(self):
        cmd = 'SELECT id, url FROM storage'
        self.__db_cursor.execute(cmd)
        result = self.__db_cursor.fetchall()
        return [(idx, self.__store.url_to_path(url)) for idx, url in result]

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


def pre_process(tuples):
    idx, full_path = tuples
    path = Path(os.path.join(full_path, 'content.txt'))
    if path.exists():
        raw_data = path.read_text()
        raw_data = BeautifulSoup(raw_data, 'lxml').getText()
        words = re.sub(r'[^А-я0-9ёЁ ]', '', raw_data).split()
        words = [morph.parse(word)[0].normal_form for word in words if word not in stopwords.words('russian')]
        result = {}
        for i, word in enumerate(words):
            if word not in result:
                result[word] = []
            result[word].append(i)
        return idx, result


def pre_process_letter(tuples):
    input, ch, dict_ch = tuples
    for idx, full_path in input:
        path = Path(os.path.join(full_path, 'content.txt'))
        if path.exists():
            raw_data = path.read_text()
            raw_data = BeautifulSoup(raw_data, 'lxml').getText()
            words = re.sub(r'[^А-я0-9ёЁ ]', '', raw_data).split()
            words = [morph.parse(word)[0].normal_form for word in words if word not in stopwords.words('russian')]
            result = {}
            for i, word in enumerate(words):
                if word[0] == ch:
                    if word not in result:
                        result[word] = []
                    result[word].append(i)
            for key, val in result.items():
                if key not in dict_ch:
                    dict_ch[key] = []
                dict_ch[key].append((idx, val))

    with open(os.path.join('./index', ch), 'wb') as f:
        try:
            pickle.dump(dict_ch, f)
        except pickle.PicklingError as e:
            logging.error(str(e))
