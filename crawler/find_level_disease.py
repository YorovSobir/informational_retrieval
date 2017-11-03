from bs4 import BeautifulSoup
import psycopg2 as pg_driver
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED
from pathlib import Path
import os
import argparse

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
    parser.add_argument('--root_dir', default='./data',
                        help='root directory where we store data')
    return parser


class FindLevel:
    def __init__(self, user, password, host, dbname, root):
        self.db = pg_driver.connect(user=user, password=password, host=host, dbname=dbname)
        self.db.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
        self.cur = self.db.cursor()
        self.store = Store(root)

    def find(self):
        cmd = "SELECT url FROM storage WHERE url LIKE '%diagnos.ru/symptoms/%'"
        self.cur.execute(cmd)
        result = self.cur.fetchall()
        for url in result:
            fullpath = self.store.url_to_path(url)
            path = Path(os.path.join(fullpath, 'content.txt'))
            if path.exists():
                data = path.read_text()
                soup = BeautifulSoup(data)
                table = soup.find('table')
                table_body = table.find('tbody')
                rows = table_body.find_all('tr')
                for idx, row in enumerate(rows):
                    if idx == 0:
                        cols = row.find_all('th')
                        if len(cols) != 3 or not (cols[0].text != 'Наименование' or cols[1].text != 'Угроза' or cols[2].text != 'Кол-во признаков'):
                            break
                    cols = row.find_all('td')
                    cols = [ele.text.strip() for ele in cols]
                    cmd = 'INSERT INTO disease VALUES(\'{0}\', \'{1}\')'.format(cols[0], cols[1])
                    self.cur.execute(cmd)
                    self.db.commit()


if __name__ == '__main__':
    parser = build_parser()
    args = parser.parse_args()
    f = FindLevel(user=args.user, password=args.password, host=args.host, dbname=args.database, root=args.root_dir)
    f.find()