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
        self.db.commit()

    def __del__(self):
        self.db.close()

    def get_id_and_url(self):
        cur = self.db.cursor()
        cur.execute('select id, url from storage')
        result = cur.fetchall()
        return result

    def set_unseen(self):
        cur = self.db.cursor()
        cur.execute('update storage set seen = false')
        try:
            self.db.commit()
        except:
            logging.error("update seen failed")
            self.db.rollback()
