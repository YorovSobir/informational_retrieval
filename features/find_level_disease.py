from bs4 import BeautifulSoup
from pathlib import Path
import os
from DBConnect import DBService
from store import Store


class FindLevel:
    def __init__(self, db_service, store):
        self.__cur = db_service.cur
        self.__store = store
        self.__db = db_service.db

    def find(self):
        cmd = "SELECT url FROM storage WHERE url LIKE '%diagnos.ru/symptoms/%'"
        self.__cur.execute(cmd)
        result = self.__cur.fetchall()
        for url in result:
            full_path = self.__store.url_to_path(url[0])
            path = Path(os.path.join(full_path, 'content.txt'))
            if path.exists():
                data = path.read_text(encoding='utf-8')
                soup = BeautifulSoup(data, 'lxml')
                tables = soup.find_all('table')
                for table in tables:
                    rows = table.find_all('tr')
                    for idx, row in enumerate(rows):
                        if idx == 0:
                            cols = row.find_all('th')
                            if len(cols) < 2 or \
                                    (cols[0].text != 'Наименование'
                                     or cols[1].text != 'Угроза'):
                                break
                        else:
                            cols = row.find_all('td')
                            cells = [cell.text.strip('\n ') for cell in cols]
                            cmd = 'INSERT INTO disease(val) VALUES(\'{0}\') ON CONFLICT("val") DO UPDATE SET val=EXCLUDED.val RETURNING id'.format(cells[0])
                            self.__cur.execute(cmd)
                            disease_id = self.__cur.fetchone()[0]
                            cmd = 'INSERT INTO levels(val) VALUES(\'{0}\') ON CONFLICT("val") DO UPDATE SET val=EXCLUDED.val RETURNING id'.format(cells[1])
                            self.__cur.execute(cmd)
                            level_id = self.__cur.fetchone()[0]
                            self.__db.commit()
                            cmd = 'INSERT INTO disease_levels VALUES({0}, {1}) ON CONFLICT DO NOTHING'.format(disease_id, level_id)
                            self.__cur.execute(cmd)
                            self.__db.commit()


if __name__ == "__main__":
    db_service = DBService(user='ir_med', password='medicine', host='localhost', dbname='ir_db')
    store = Store('./data')
    fl = FindLevel(db_service, store)
    fl.find()
