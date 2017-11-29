import pickle
import logging
import os
from pathlib import Path
from multiprocessing import Pool, Lock, Manager
import multiprocessing
from utils.utils import id_and_path_to_doc

letters = [chr(i) for i in range(ord('а'), ord('я') + 1)]
# letters.extend([chr(i + 48) for i in range(10)])
letters.append('ё')
locks = {key: Lock() for key in letters}

manager = Manager()


class Index:
    def __init__(self, db_service, data_dir, index_dir):
        self.__db_service = db_service
        self.__data_dir = data_dir
        self.__index_dir = index_dir
        self.__common_dict = {key: dict() for key in letters}

    def merge(self, id_and_path, result):
        for idx, elem in enumerate(result):
            for key, value in elem.items():
                for word, positions in value.items():
                    if word not in self.__common_dict[key]:
                        self.__common_dict[key][word] = []
                    self.__common_dict[key][word].append((id_and_path[idx][0], positions))

    def build_index(self):
        id_and_path = id_and_path_to_doc(self.__data_dir, self.__db_service)
        with Pool(multiprocessing.cpu_count()) as p:
            while id_and_path:
                result = p.map(index, id_and_path)
                self.merge(id_and_path, result)
                id_and_path = id_and_path_to_doc(self.__data_dir, self.__db_service)

    def serialize(self):
        for key in self.__common_dict:
            with open(os.path.join(self.__index_dir, key + '.ind'), 'wb') as f:
                try:
                    pickle.dump(self.__common_dict[key], f)
                except pickle.PicklingError as e:
                    logging.error(str(e))

    def deserialize(self, file_name):
        with open(os.path.join(self.__index_dir, file_name), 'rb') as f:
            try:
                self.__common_dict = pickle.load(f)
            except pickle.UnpicklingError as e:
                logging.error(str(e))


def index(data_input):
    idx, path_str = data_input
    full_path = os.path.join(path_str, 'words')
    path = Path(full_path)
    dicts = {key: dict() for key in letters}
    if path.exists():
        with open(full_path, 'rb') as f:
            words = []
            try:
                words = pickle.load(f)
            except pickle.UnpicklingError as e:
                logging.error(str(e))
            for i, word in enumerate(words):
                if word:
                    dict_letter = dicts[word[0]]
                    if word not in dict_letter:
                        dict_letter[word] = 0
                    dict_letter[word] += 1
    else:
        logging.warning("cannot find file " + full_path)
    return dicts
