import pickle
import logging
import os
from pathlib import Path
from multiprocessing import Pool, Lock
import multiprocessing
from utils.utils import id_and_path_to_doc

letters = [chr(i) for i in range(ord('а'), ord('я') + 1)]
letters.extend([chr(i + 48) for i in range(10)])
letters.append('ё')
locks = {key: Lock() for key in letters}


class Index:
    def __init__(self, db_service, data_dir, index_dir):
        self.__db_service = db_service
        self.__db_service.set_unseen()
        self.__data_dir = data_dir
        self.__index_dir = index_dir
        self.__common_dict = {key: dict() for key in letters}

    def build_index(self):
        id_and_path = id_and_path_to_doc(self.__data_dir, self.__db_service)
        with Pool(multiprocessing.cpu_count()) as p:
            while id_and_path:
                p.map(index, [(idx, path, self.__common_dict) for idx, path in id_and_path])
                id_and_path = id_and_path_to_doc(self.__data_dir, self.__db_service)

    def serialize(self, file_name):
        with open(os.path.join(self.__index_dir, file_name), 'wb') as f:
            try:
                pickle.dump(self.__common_dict, f)
            except pickle.PicklingError as e:
                logging.error(str(e))

    def deserialize(self, file_name):
        with open(os.path.join(self.__index_dir, file_name), 'rb') as f:
            try:
                self.__common_dict = pickle.load(f)
            except pickle.UnpicklingError as e:
                logging.error(str(e))


def index(data_input):
    idx, path_str, dicts = data_input
    full_path = os.path.join(path_str, 'words')
    path = Path(full_path)
    if path.exists():
        with open(full_path, 'rb') as f:
            words = []
            try:
                words = pickle.load(f)
            except pickle.UnpicklingError as e:
                logging.error(str(e))

            for i, word in enumerate(words):
                if word:
                    lock_letter = locks[word[0]]
                    dict_letter = dicts[word[0]]
                    if lock_letter:
                        with lock_letter:
                            if word not in dict_letter:
                                dict_letter[word] = []
                            dict_letter[word].append(i)
                    else:
                        logging.warning("cannot find lock object for " + word)
    else:
        logging.warning("cannot find file " + full_path)
