import sys
import os
sys.path.append(os.getcwd() + "/../")

from ranking.BM25 import BM25
import logging
from utils.db_service import DBService
from utils.utils import url_to_path
import argparse


class Find:
    def __init__(self, db_service, path_to_ind, path_to_doc):
        self.bm25 = BM25(path_to_ind, path_to_doc)
        self.db = db_service.db
        self.cur = self.db.cursor()
        cmd = """
            SELECT
                id_document
            FROM disease_treatment
        """
        self.cur.execute(cmd)
        self.docs_disease = set([x[0] for x in self.cur.fetchall()])
        # print(self.docs_disease)

    def find_cards(self, query):
        docs = self.bm25.get_documents(query)
        result = []
        for doc in docs:
            if doc in self.docs_disease:
                cmd = """
                        SELECT
                          ST.url,
                          DT.disease,
                          DT.treatment,
                          T.l_val
                        FROM (SELECT
                                D.val AS d_val,
                                L.val AS l_val
                              FROM disease_levels AS DL
                                JOIN disease AS D ON DL.id_disease = D.id
                                JOIN levels AS L ON DL.id_level = L.id) AS T
                          RIGHT JOIN disease_treatment AS DT ON LOWER(DT.disease) LIKE concat('%', LOWER(T.d_val), '%')
                           JOIN storage as ST ON ST.id = DT.id_document WHERE DT.id_document = {0}
                      """.format(doc)
                self.cur.execute(cmd)
                res = self.cur.fetchall()
                if res:
                    result.append(res)
            if len(result) >= 10:
                break
        return result

    def find_ranked(self, query):
        return self.bm25.get_documents(query)


def build_parser():
    parser = argparse.ArgumentParser(add_help=False, description='Index for our information retrieval system')
    parser.add_argument('-h', '--host', default='192.168.1.225',
                        help='address of postgres server')
    parser.add_argument('-p', '--port', default='5432',
                        help='port of postgres server')
    parser.add_argument('-d', '--database', default='ir_db',
                        help='database to connect')
    parser.add_argument('-u', '--user', default='ir_med',
                        help='user in postgres server')
    parser.add_argument('--password', default='medicine',
                        help='password for user in postgres server')
    parser.add_argument('--index_dir', default='../index2',
                        help='directory where we store index')
    parser.add_argument('--data_dir', default='../data',
                        help='directory where we store data')
    return parser


def get_cards(query):
    # log_format = '%(asctime) -15s %(levelname)s:%(message)s'
    # logging.basicConfig(filename='./log/find.log', level=logging.DEBUG, format=log_format)
    parser = build_parser()
    args = parser.parse_args()
    db_service = DBService(user=args.user, password=args.password, host=args.host, dbname=args.database)
    f = Find(db_service, args.index_dir, args.data_dir)
    return f.find_cards(query)


def get_ranked(query):
    # log_format = '%(asctime) -15s %(levelname)s:%(message)s'
    # logging.basicConfig(filename='./log/find.log', level=logging.DEBUG, format=log_format)
    parser = build_parser()
    args = parser.parse_args()
    db_service = DBService(user=args.user, password=args.password, host=args.host, dbname=args.database)
    f = Find(db_service, args.index_dir, args.data_dir)
    return f.find_ranked(query)
