import argparse
import logging
from utils.db_service import DBService
from index import Index
import multiprocessing
from multiprocessing import Pool

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
    parser.add_argument('--data_dir', default='../data',
                        help='directory where we store data')
    parser.add_argument('--index_dir', default='./index',
                        help='directory where we store index')
    return parser


def main():
    log_format = '%(asctime) -15s %(levelname)s:%(message)s'
    logging.basicConfig(filename='./log/index.log', level=logging.DEBUG, format=log_format)
    parser = build_parser()
    args = parser.parse_args()
    db_service = DBService(user=args.user, password=args.password, host=args.host, dbname=args.database)

    index = Index(db_service, args.data_dir, args.index_dir)
    index.build_index()
    index.serialize()


manager = multiprocessing.Manager()


def f(input):
    x, l = input
    l[x] = []
    l[x].append(x)


if __name__ == '__main__':
    l = manager.dict()
    with Pool(3) as pool:
        pool.map(f, [(x, l) for x in range(10)])
    print(l)
    # main()
