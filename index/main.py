import argparse
import logging
import sys
sys.path.append('..')
from utils.db_service import DBService
from index.index import Index


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


def index(db, data_dir, index_dir):
    log_format = '%(asctime) -15s %(levelname)s:%(message)s'
    logging.basicConfig(filename='./log/index.log', level=logging.DEBUG, format=log_format)
    index_ = Index(db, data_dir, index_dir)
    index_.build_index()
    index_.serialize()
    # index_.deserialize('a.ind')


def main():
    parser = build_parser()
    args = parser.parse_args()
    db_service = DBService(user=args.user, password=args.password, host=args.host, dbname=args.database)
    index(db_service, args.data_dir, args.index_dir)

