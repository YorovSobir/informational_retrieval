from ranking.BM25 import BM25
import argparse
import logging
from utils.db_service import DBService
from utils.utils import url_to_path


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
    parser.add_argument('--index_dir', default='./index2',
                        help='directory where we store index')
    parser.add_argument('--data_dir', default='./data',
                        help='directory where we store data')
    return parser


class Find:
    def __init__(self, db_service, path_to_ind, path_to_doc):
        self.bm25 = BM25(path_to_ind, path_to_doc)
        self.db = db_service.db
        self.cur = self.db.cursor()

    def find(self, q):
        docs = self.bm25.get_documents(q, 10)
        # result = []
        # for doc in docs:
        #     cmd = """
        #             SELECT
        #               DT.disease,
        #               DT.treatment,
        #               T.l_val
        #             FROM (SELECT
        #                     D.val AS d_val,
        #                     L.val AS l_val
        #                   FROM disease_levels AS DL
        #                     JOIN disease AS D ON DL.id_disease = D.id
        #                     JOIN levels AS L ON DL.id_level = L.id) AS T
        #               RIGHT JOIN disease_treatment AS DT ON LOWER(DT.disease) = LOWER(T.d_val)
        #             WHERE DT.id_document={0}
        #            """.format(doc)
        #     self.cur.execute(cmd)
        #     result.append(self.cur.fetchall())
        return docs


def main():
    log_format = '%(asctime) -15s %(levelname)s:%(message)s'
    logging.basicConfig(filename='./log/find.log', level=logging.DEBUG, format=log_format)
    parser = build_parser()
    args = parser.parse_args()
    db_service = DBService(user=args.user, password=args.password, host=args.host, dbname=args.database)
    result = db_service.get_id_and_url()
    # data = [url_to_path(url, args.data_dir) + 'words' for _, url in result]
    f = Find(db_service, args.index_dir, args.data_dir)
    print(f.find('Сильно болит голова'))


if __name__ == "__main__":
    main()
