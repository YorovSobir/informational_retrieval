import pymorphy2
import re
from nltk.corpus import stopwords
import math
import pickle
from pathlib import Path

letters = [chr(i) for i in range(ord('а'), ord('я') + 1)]
letters.extend([chr(i + 48) for i in range(10)])
letters.append('ё')


class BM25:
    def __init__(self, path_to_ind, path_to_doc, k1=2.0, b=0.75):
        self.path_to_ind = path_to_ind
        self.path_array = path_to_doc
        self.k1 = k1
        self.b = b
        self.cashe = {}
        self.ind = {}
        self.morph = pymorphy2.MorphAnalyzer()
        self.avg = None
        self.idf_cashe = {}

    def get_documents(self, q, count=10):
        words = re.sub(r'[^А-яёЁ ]', '', q).split()
        words = [self.morph.parse(word)[0].normal_form for word in words]
        words = [word for word in words if word not in stopwords.words('russian')]
        h = hash(tuple(words))
        if h in self.cashe:
            return self.cashe[h][:count]
        result = 0.0
        for word in words:
            idf = self.__idf(word)
            tf = self.__TF(word)
            word_count = self.__word_count(doc)
            avg = self.__avgdl()
            result += self.__idf(word) * self.__TF(word) * (self.k1 + 1) / \
                                  (self.__TF(word) + self.k1 * (1 - self.b + self.b * self.__word_count(doc) / self.__avgdl()))

        # for doc in self.__next_doc():
        #     result = 0.0
        #     for word in words:
        #         result += self.__idf(word) * self.__TF(word, doc) * (self.k1 + 1) / \
        #                   (self.__TF(word, doc) + self.k1 * (1 - self.b + self.b * self.__word_count(doc) / self.__avgdl()))
        #     if words not in self.cashe:
        #         self.cashe[h] = list()
        #     self.cashe[h] = (doc, result)
        self.cashe[h] = sorted(self.cashe[h], key=lambda x: x[1])
        return self.cashe[h][:count]

    def __avgdl(self):
        if self.avg is not None:
            return self.avg
        self.avg = 0
        for path in self.path_array:
            file = Path(path)
            if file.exists():
                self.avg += self.__word_count(file.read_text(encoding='utf-8'))
        self.avg /= self.__N()
        return self.avg

    def __word_count(self, doc):
        return len(doc.split())

    def __idf(self, word):
        if word in self.idf_cashe:
            return self.idf_cashe[word]
        self.idf_cashe[word] = math.log2((self.__N() - self.__n(word) + 0.5) / (self.__n(word) + 0.5))
        return self.idf_cashe[word]

    def __load_ind(self, letter):
        self.__ind = pickle.load(open(self.path_to_ind + "/" + letter + ".ind", 'rb'))

    def __next_doc(self):
        for path in self.path_array:
            with open(path, 'rb') as f:
                unpickler = pickle.Unpickler(f)
                try:
                    data = unpickler.load()
                except EOFError:
                    data = list()
                yield data
            # file = Path(path)
            # if file.exists():
            #     yield file.read_text()

    def __N(self):
        return len(self.path_array)

    def __n(self, word):
        self.__load_ind(word[0])
        if word in self.ind:
            return len(self.ind[word][1])
        return 0

    def __TF(self, word, doc):
        self.__load_ind(word[0])
        if word in self.ind:
            for d in self.ind[word]:
                if d[0] == doc:
                    return len(d[1])
        return 0
