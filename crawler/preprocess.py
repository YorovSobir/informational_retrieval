import re
from nltk.corpus import stopwords
import pymorphy2
from bs4 import BeautifulSoup
from collections import Counter
import pickle


class Index:
    def __init__(self, db_cursor):
        self.__db_cursor = db_cursor

    def create(self):
        data = []
        [data.append(self.__tokens(i, doc)) for i, doc in self.__next_document()]
        common_dict = {}
        for doc_id, words in data:
            for word in words.keys():
                if word not in common_dict:
                    common_dict[word] = []
                common_dict[word] += [(doc_id, words[word])]
        return common_dict

    def __next_document(self):
        data1 = 'Словари распространяются 13 отдельными пакетами 13'
        data2 = 'Словари отдельными пакетами 13'
        yield 1, data1
        yield 2, data2

    def __tokens(self, i, raw_data):
        morph = pymorphy2.MorphAnalyzer()
        raw_data = BeautifulSoup(raw_data, 'lxml').text
        words = re.sub(r'[^А-я0-9ёЁ ]', '', raw_data).lower().split()
        words = [morph.parse(word)[0].normal_form for word in words]
        words = [word for word in words if word not in stopwords.words('russian')]
        words_to_count = Counter(words)
        result = {}
        for word in words_to_count.keys():
            result[word] = [idx for idx, x in enumerate(words) if x == word]
        return i, result


if __name__ == "__main__":
    a = Index(None)
    pickle.dump(a.create(), open('index.ind', 'wb'))
