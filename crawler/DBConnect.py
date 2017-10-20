import psycopg2 as pg_driver
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED
import logging


class DBService:
    def __init__(self, user, password, host, dbname):
        self.db = pg_driver.connect(user=user, password=password, host=host, dbname=dbname)
        self.db.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
        self.cur = self.db.cursor()
        self.cur.execute('create table if not exists storage '
                         '(url text primary key, update_count integer, '
                         'last_update date, bdata bytea)')
        self.cur.execute('create table if not exists urls '
                         '(url text primary key, seen boolean)')
        self.cur.execute('create table if not exists base (url text primary key)')
        self.db.commit()

    def __del__(self):
        self.db.close()

    def get_url(self, n=1):
        self.cur.execute('select url from urls where seen = false limit {0} for update'.format(n))
        result = self.cur.fetchall()
        for p in result:
            try:
                self.cur.execute('update urls set seen = true where url = \'{0}\''.format(p[0]))
            except pg_driver.Error:
                logging.warning("it's unreal " + str(p[0]))
        try:
            self.db.commit()
        except:
            logging.error("commit failed")
            self.db.rollback()
            return []
        return [x[0] for x in result]

    def is_empty(self):
        self.cur.execute('SELECT COUNT(*) FROM urls where seen = false')
        result = self.cur.fetchone()
        return result[0] == 0

    def add_url(self, urls):
        for url in urls:
            try:
                cmd = 'INSERT INTO urls VALUES (\'{0}\', false) ON CONFLICT DO NOTHING'.format(url)
                self.cur.execute(cmd)
                self.db.commit()
            except pg_driver.Error as e:
                logging.warning('Can\'t add url ' + url + ' code ' + e.pgcode)
                self.db.rollback()

    def size(self):
        self.cur.execute('SELECT COUNT(*) FROM storage')
        result = self.cur.fetchone()
        return result[0]

    def add_data(self, url, data):
        try:
            cmd = 'INSERT INTO storage(url, update_count, last_update, bdata) ' \
                  'VALUES (\'{0}\', 0, now(), {1}) ON CONFLICT (url) DO UPDATE ' \
                  'SET update_count = storage.update_count + 1, last_update=now(), bdata={1}'\
                .format(url, pg_driver.Binary(data))
            self.cur.execute(cmd)
            self.db.commit()
        except pg_driver.Error as e:
            logging.error('Can\'t insert url ' + url + ' code ' + e.pgcode)
            self.db.rollback()

    def get_base(self):
        self.cur.execute('SELECT url FROM base')
        result = self.cur.fetchall()
        out = []
        for i in result:
            if len(i) > 0:
                out.append(i[0])
        return out

    def add_base(self, base):
        for url in base:
            try:
                self.cur.execute('INSERT INTO base(url) VALUES (\'{0}\')'.format(url))
                self.db.commit()
            except pg_driver.Error as e:
                logging.warning('Can\'t add to base url ' + url + ' code ' + e.pgcode)
                self.db.rollback()
