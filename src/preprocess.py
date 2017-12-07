import os
from bs4 import BeautifulSoup
from multiprocessing import Pool
import multiprocessing
from pathlib import Path
import pickle
import logging

from utils.db_service import DBService
from utils.utils import id_and_path_to_doc, preprocess_text, build_parser


class Data:
    def __init__(self, db_service, data_dir):
        self.__db_service = db_service
        self.__db_service.set_unseen()
        self.__data_dir = data_dir

    def preprocess(self):
        id_and_path = id_and_path_to_doc(self.__data_dir, self.__db_service)
        with Pool(multiprocessing.cpu_count()) as p:
            while id_and_path:
                p.map(clean_doc, id_and_path)
                id_and_path = id_and_path_to_doc(self.__data_dir, self.__db_service)

    def doc_count(self):
        id_and_path = id_and_path_to_doc(self.__data_dir, self.__db_service)
        result = {}
        while id_and_path:
            for id, full_path in id_and_path:
                try:
                    with open(os.path.join(full_path, 'words'), 'rb') as f:
                        unpickler = pickle.Unpickler(f)
                        try:
                            result[id] = len(unpickler.load())
                        except EOFError:
                            result[id] = 0
                except:
                    result[id] = 0
            id_and_path = id_and_path_to_doc(self.__data_dir, self.__db_service)
        with open(os.path.join(self.__data_dir, 'count'), 'wb') as f:
            try:
                pickle.dump(result, f)
            except pickle.PicklingError as e:
                logging.error(str(e))


def clean_doc(document):
    idx, full_path = document
    path = Path(os.path.join(full_path, 'content.txt'))
    if path.exists():
        raw_data = path.read_text(encoding='utf-8')
        raw_data = BeautifulSoup(raw_data, 'lxml').getText()
        raw_data_path = Path(os.path.join(full_path, 'content_without_tags.txt'))
        raw_data_path.write_text(raw_data, encoding='utf-8')
        words = preprocess_text(raw_data)
        with open(os.path.join(full_path, 'words'), 'wb') as f:
            try:
                pickle.dump(words, f)
            except pickle.PicklingError as e:
                logging.error(str(e))
    else:
        logging.warning("file not found: " + os.path.join(full_path, 'content.txt'))


def preprocess(db, data_dir):
    # Data(db, data_dir).preprocess()
    Data(db, data_dir).doc_count()


if __name__ == '__main__':
    parser = build_parser()
    args = parser.parse_args()
    log_format = '%(asctime) -15s %(levelname)s:%(message)s'
    logging.basicConfig(filename='./log/preprocess.log', level=logging.DEBUG, format=log_format)
    db_service = DBService(user=args.user, password=args.password, host=args.host, dbname=args.database)
    preprocess(db_service, args.data_dir)
