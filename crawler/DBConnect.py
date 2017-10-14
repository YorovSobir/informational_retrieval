import psycopg2 as pg_driver
import logging

class DBService:
    def __init__(self, user, password, host, dbname):
        self.db = pg_driver.connect(user=user, password=password, host=host, dbname=dbname)
        self.db.set_isolation_level(1)
        self.cur = self.db.cursor()

    def __del__(self):
        self.db.close()

    def get_url(self, n=1):
        self.cur.execute('SELECT get_url({0})'.format(n))
        result = self.cur.fetchone()
        self.db.commit()
        if result[0] is None:
            return []
        return result[0]

    def is_empty(self):
        self.cur.execute('SELECT COUNT(*) FROM urls')
        result = self.cur.fetchone()
        return result[0] == 0

    def add_url(self, urls):
        for url in urls:
            try:
                cmd = 'SELECT url FROM old_urls WHERE url=\'{0}\''.format(url)
                self.cur.execute('LOCK urls IN SHARE ROW EXCLUSIVE MODE')
                self.cur.execute(cmd)
                if self.cur.fetchone() is None:
                    cmd = 'INSERT INTO urls(url) VALUES (\'{0}\')'.format(url)
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
        self.cur.execute('SELECT COUNT(*) FROM storage WHERE url=\'{0}\''.format(url))
        count = self.cur.fetchone()
        if count[0] == 0:
            try:
                cmd = 'INSERT INTO storage(url, update_count, last_update, bdata) VALUES (\'{0}\', 0, now(), {1})'\
                    .format(url, pg_driver.Binary(data))
                self.cur.execute(cmd)
                self.db.commit()
            except pg_driver.Error as e:
                logging.error('Can\'t insert url ' + url + ' code ' + e.pgcode)
                self.db.rollback()
        else:
            try:
                cmd = 'UPDATE storage SET update_count=update_count+1, last_update=now(), bdata={0} WHERE url=\'{1}\''\
                    .format(pg_driver.Binary(data), url)
                self.cur.execute(cmd)
                self.db.commit()
            except pg_driver.Error as e:
                logging.error('Can\'t update url ' + url + ' code ' + e.pgcode)
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
