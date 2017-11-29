import argparse
import logging
import os
from utils.db_service import DBService
from crawler.main import crawler
from index.main import index
from preprocess.main import preprocess


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
                        help='root directory where we store data')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-c', action='store_true', help='run crawler')
    group.add_argument('-i', action='store_true', help='run index')
    group.add_argument('--pr', action='store_true', help='run preprocess')
    return parser


if __name__ == '__main__':
    log_format = '%(asctime) -15s %(levelname)s:%(message)s'
    logging.basicConfig(filename='./log/base.log', level=logging.DEBUG, format=log_format)
    parser = build_parser()
    args = parser.parse_args()
    db = DBService(user=args.user, password=args.password, host=args.host, dbname=args.database)
    if args.c:
        crawler(db, args.urls, os.path.abspath(args.data_dir))
    elif args.i:
        index(db, os.path.abspath(args.data_dir), os.path.abspath(args.index_dir))
    elif args.pr:
        preprocess(db, os.path.abspath(args.data_dir))
