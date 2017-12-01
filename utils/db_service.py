import psycopg2 as pg_driver
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED
import logging


class DBService:
    def __init__(self, user, password, host, dbname):
        self.db = pg_driver.connect(user=user, password=password, host=host, dbname=dbname)
        self.db.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
        cur = self.db.cursor()
        cur.execute('create table if not exists storage '
                    '(id serial primary key, url text unique, update_count integer, '
                    'last_update date, incoming_link integer, seen Boolean)')
        cur.execute('create table if not exists urls '
                    '(url text primary key, seen boolean)')
        cur.execute('create table if not exists base (url text primary key)')
        cur.execute('create table if not exists disease (id INTEGER PRIMARY KEY, val text)')
        cur.execute('create table if not exists doc_disease_treatment (document_id INTEGER REFERENCES storage(id), disease_id INTEGER REFERENCES disease(id), treatment TEXT)')
        cur.execute('create table if not exists levels (id INTEGER PRIMARY KEY, val text)')
        cur.execute('create table if not exists disease_levels (disease_id INTEGER REFERENCES disease(id), level_id INTEGER REFERENCES levels(id))')
        self.db.commit()

    def __del__(self):
        self.db.close()

    def get_id_and_url(self, n=25000):
        cur = self.db.cursor()
        cur.execute('select id, url from storage where seen = false limit {0} for update'.format(n))
        result = cur.fetchall()
        for p in result:
            try:
                cur.execute('update storage set seen = true where id = \'{0}\''.format(p[0]))
            except pg_driver.Error:
                logging.warning("unexpected failed: cannot change seen field for " + str(p[0]))
        try:
            self.db.commit()
        except:
            logging.error("commit failed")
            self.db.rollback()
            return []

        return result

    def set_unseen(self):
        cur = self.db.cursor()
        cur.execute('update storage set seen = false')
        try:
            self.db.commit()
        except:
            logging.error("update seen failed")
            self.db.rollback()

    # used by crawler
    def get_url(self, n=1):
        cur = self.db.cursor()
        cur.execute('select url from urls where seen = false limit {0} for update'.format(n))
        result = cur.fetchall()
        for p in result:
            try:
                cur.execute('update urls set seen = true where url = \'{0}\''.format(p[0]))
            except pg_driver.Error:
                logging.warning("it's unreal " + str(p[0]))
        try:
            self.db.commit()
        except:
            logging.error("commit failed")
            self.db.rollback()
            return []
        return [x[0] for x in result]

    def add_url(self, urls):
        cur = self.db.cursor()
        for url in urls:
            try:
                cmd = 'INSERT INTO urls VALUES (\'{0}\', false) ON CONFLICT DO NOTHING'.format(url)
                cur.execute(cmd)
                self.db.commit()
            except pg_driver.Error as e:
                logging.warning('Can\'t add url ' + url + ' code ' + e.pgcode)
                self.db.rollback()

    def add_data(self, url):
        cur = self.db.cursor()
        try:
            cmd = 'INSERT INTO storage(url, update_count, last_update, incoming_link, seen) ' \
                  'VALUES (\'{0}\', 0, now(), 0, false) ' \
                  'ON CONFLICT (url) DO UPDATE ' \
                  'SET update_count = storage.update_count + 1, last_update=now()' \
                .format(url)
            cur.execute(cmd)
            self.db.commit()
        except pg_driver.Error as e:
            logging.error(str(e))
            self.db.rollback()

    def get_base(self):
        cur = self.db.cursor()
        cur.execute('SELECT url FROM base')
        result = cur.fetchall()
        out = []
        for i in result:
            if len(i) > 0:
                out.append(i[0])
        return out

    def add_base(self, base):
        cur = self.db.cursor()
        for url in base:
            try:
                cur.execute('INSERT INTO base(url) VALUES (\'{0}\')'.format(url))
                self.db.commit()
            except pg_driver.Error as e:
                logging.warning('Can\'t add to base url ' + url + ' code ' + e.pgcode)
                self.db.rollback()

    def update_incoming_links(self, links):
        cur = self.db.cursor()
        for p in links:
            try:
                cur.execute('update storage set '
                            'incoming_link = incoming_link + 1 '
                            'where url = \'{0}\''.format(p))
            except pg_driver.Error as e:
                logging.warning(str(e))
        try:
            self.db.commit()
        except:
            logging.error("update commit failed")
            self.db.rollback()

if __name__ == '__main__':
    DBService(user='ir_med', password='medicine', host='localhost', dbname='ir_db')
