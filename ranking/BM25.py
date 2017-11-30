import pymorphy2
import re
from nltk.corpus import stopwords
import math
import pickle
from pathlib import Path
import os
import logging

letters = [chr(i) for i in range(ord('а'), ord('я') + 1)]
letters.append('ё')


class BM25:
    def __init__(self, path_to_ind, path_to_doc, k1=2.0, b=0.75):
        self.path_to_ind = path_to_ind
        self.path_to_doc = path_to_doc
        self.k1 = k1
        self.b = b
        self.cashe = {}
        self.ind = {key: dict() for key in letters}
        self.morph = pymorphy2.MorphAnalyzer()
        self.doc = {}
        self.avg = self.__avgdl()
        self.__load_ind()

    def get_documents(self, q, count=10):
        words = re.sub(r'[^А-яёЁ ]', ' ', q).split()
        words = [self.morph.parse(word)[0].normal_form for word in words]
        words = [word for word in words if word not in stopwords.words('russian')]
        h = hash(tuple(words))
        list_doc = set()
        for word in words:
            let = word[0]
            try:
                for d in self.ind[let][word]:
                    list_doc.add(d[0])
            except:
                pass
        self.cashe[h] = list()
        print (len(list_doc))
        for doc in list(list_doc)[:20]:
            bm25 = 0.0
            for word in words:
                print(self.__TF(word, doc))
                bm25 += self.__idf(word) * (self.__TF(word, doc) * (self.k1 + 1)) / (self.__TF(word, doc) + self.k1 * (1 - self.b + self.b * self.__word_count(doc) / self.avg))
            # print (bm25)
            self.cashe[h].append((doc, bm25))
        return self.cashe[h]

    def __avgdl(self):
        self.avg = 0
        with open(os.path.join(self.path_to_doc, 'count'), 'rb') as f:
            try:
                self.doc = pickle.load(f)
                for i in self.doc.keys():
                    self.avg += self.doc[i]
                self.avg /= len(self.doc.keys())
            except pickle.PicklingError as e:
                logging.error(str(e))
        return self.avg

    def __word_count(self, i):
        return self.doc[i]

    def __idf(self, word):
        return math.log2((self.__N() - self.__n(word) + 0.5) / (self.__n(word) + 0.5))

    def __load_ind(self):
        for key in self.ind:
            with open(os.path.join(self.path_to_ind, key + '.ind'), 'rb') as f:
                try:
                    self.ind[key] = pickle.load(f)
                except pickle.PicklingError as e:
                    logging.error(str(e))

    def __N(self):
        return len(self.doc.keys())

    def __n(self, word):
        let = word[0]
        try:
            count = len(self.ind[let][word])
        except:
            count = 0
        return count

    def __TF(self, word, doc):
        let = word[0]
        l = self.ind[let][word]
        for i in l:
            if i[0] == doc:
                return l[1]
        return 0
