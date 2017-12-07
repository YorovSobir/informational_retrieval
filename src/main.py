import argparse
import logging
import os
from utils.db_service import DBService
from crawler import crawler
from index import index
from preprocess import preprocess


def build_parser():
    parser = argparse.ArgumentParser(add_help=False, description='Data for database connection')
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
    parser.add_argument('--urls', nargs='*',
                        help='base urls where will start')
    parser.add_argument('--data_dir', default='./data',
                        help='root directory where we store data')
    parser.add_argument('--index_dir', default='./index_data',
                        help='root directory where we store index')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--crawler', action='store_true', help='run crawler')
    group.add_argument('--index', action='store_true', help='build index')
    group.add_argument('--preprocess', action='store_true', help='run preprocess')
    return parser


if __name__ == '__main__':
    log_format = '%(asctime) -15s %(levelname)s:%(message)s'
    parser = build_parser()
    args = parser.parse_args()
    db = DBService(user=args.user, password=args.password, host=args.host, dbname=args.database)
    if args.crawler:
        logging.basicConfig(filename='./log/crawler.log', level=logging.DEBUG, format=log_format)
        crawler(db, args.urls, os.path.abspath(args.data_dir))
    elif args.index:
        logging.basicConfig(filename='./log/index.log', level=logging.DEBUG, format=log_format)
        index(db, os.path.abspath(args.data_dir), os.path.abspath(args.index_dir))
    elif args.preprocess:
        print('Start preprocess')
        logging.basicConfig(filename='./log/preprocess.log', level=logging.DEBUG, format=log_format)
        preprocess(db, os.path.abspath(args.data_dir))
