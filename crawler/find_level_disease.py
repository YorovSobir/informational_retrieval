from bs4 import BeautifulSoup
from pathlib import Path
import os


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
                data = path.read_text()
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
                            cmd = 'INSERT INTO disease VALUES(\'{0}\', \'{1}\')'.format(cells[0], cells[1])
                            self.__cur.execute(cmd)
                            self.__db.commit()
