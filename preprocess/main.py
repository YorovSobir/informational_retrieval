import argparse
import logging
from data import Data
from utils.db_service import DBService


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
    return parser


def main():
    log_format = '%(asctime) -15s %(levelname)s:%(message)s'
    logging.basicConfig(filename='./log/preprocess.log', level=logging.DEBUG, format=log_format)
    parser = build_parser()
    args = parser.parse_args()
    db_service = DBService(user=args.user, password=args.password, host=args.host, dbname=args.database)
    data = Data(db_service, args.data_dir)
    # data.doc_count()
    data.preprocess()


if __name__ == '__main__':
    main()
