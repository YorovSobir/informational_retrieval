import argparse
import logging
from DBConnect import DBService
from find_level_disease import FindLevel
from index import Index
from spider import Spider
from urllib.parse import urlparse

from store import Store


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
    parser.add_argument('--root_dir', default='./data',
                        help='root directory where we store data')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--crawler', action='store_true', help='run crawler instead of index')
    group.add_argument('--index', action='store_true', help='run index instead of crawler')
    return parser


def main():
    log_format = '%(asctime) -15s %(levelname)s:%(message)s'
    logging.basicConfig(filename='./log/crawler.log', level=logging.DEBUG, format=log_format)
    parser = build_parser()
    args = parser.parse_args()
    db_service = DBService(user=args.user, password=args.password, host=args.host, dbname=args.database)
    store = Store(args.root_dir)
    if args.index:
        FindLevel(db_service, store).find()
        index = Index(db_service, store, './index')
        index.create()
        index.serialize('index.ind')
    else:
        urls_domain = []
        for url in args.urls:
            try:
                domain = urlparse(url)
            except ValueError as e:
                logging.warning(str(e))
                continue
            urls_domain.append(domain.netloc)
        db_service.add_base(urls_domain)
        db_service.add_url(args.urls)
        Spider(db_service, store).spider()


if __name__ == '__main__':
    main()
