import psycopg2 as pg_driver


class DBService:
    def __init__(self, user, password, host, dbname):
        self.db = pg_driver.connect(user=user, password=password, host=host, dbname=dbname)
        self.cur = self.db.cursor()

    def __del__(self):
        self.db.close()

    def get_url(self):
        self.cur.execute('SELECT url FROM urls limit 1')
        result = self.cur.fetchone()
        if result is not None:
            self.cur.execute('DELETE FROM urls WHERE url=\'{0}\''.format(result[0]))
            self.db.commit()
        return result

    def is_empty(self):
        self.cur.execute('SELECT COUNT(*) FROM urls')
        result = self.cur.fetchone()
        return result[0] == 0

    def add_url(self, urls):
            for url in urls:
                try:
                    cmd = 'INSERT INTO urls(url) VALUES (\'{0}\')'.format(url)
                    self.cur.execute(cmd)
                    self.db.commit()
                except:
                    self.db.rollback()

    def size(self):
        self.cur.execute('SELECT COUNT(*) FROM storage')
        result = self.cur.fetchone()
        return result[0]

    def add_data(self, url, data):
        self.cur.execute('SELECT COUNT(*) FROM storage WHERE url=\'{0}\''.format(url))
        count = self.cur.fetchone()
        if count[0] == 0:
            cmd = 'INSERT INTO storage(url, update_count, last_update, bdata) VALUES (\'{0}\', 0, now(), {1})'\
                .format(url, pg_driver.Binary(data))
            self.cur.execute(cmd)
            self.db.commit()
        else:
            cmd = 'UPDATE storage SET update_count=update_count+1, last_update=now(), bdata={0} WHERE url=\'{1}\''\
                .format(pg_driver.Binary(data), url)
            self.cur.execute(cmd)
            self.db.commit()

    # def get(self, url):
    #     self.cur.execute('SELECT bdata FROM storage WHERE url=\'{0}\''.format(url))
    #     print(self.cur.fetchone()[0].encode())
