import sys
import argparse
import logging
from DBConnect import DBService
from spider import spider


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
    parser.add_argument('--urls', nargs='+', required=True,
                        help='base urls where will start')

    return parser


def main():
    log_format = '%(asctime) -15s %(levelname)s:%(message)s'
    logging.basicConfig(filename='./log/crawler.log', level=logging.DEBUG, format=log_format)
    parser = build_parser()
    args = parser.parse_args()
    db_cursor = DBService(user=args.user, password=args.password, host=args.host, dbname=args.database)
    db_cursor.add_base(args.urls)
    db_cursor.add_url(args.urls)
    spider(db_cursor)


if __name__ == '__main__':
    main()
