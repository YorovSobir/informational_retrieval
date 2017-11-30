from time import time

import pymorphy2
import re
from nltk.corpus import stopwords
import math
import pickle
import os
import logging
import heapq

letters = [chr(i) for i in range(ord('а'), ord('я') + 1)]
letters.append('ё')


class BM25:
    def __init__(self, index_dir, data_dir, k1=2.0, b=0.75):
        self.index_dir = index_dir
        self.data_dir = data_dir
        self.k1 = k1
        self.b = b
        self.cashe = {}
        self.ind = {key: dict() for key in letters}
        self.morph = pymorphy2.MorphAnalyzer()
        self.doc = {}
        self.avg = self.__avgdl()
        # self.__load_ind()

    def get_documents(self, q, count=10):
        words = re.sub(r'[^А-яёЁ ]', ' ', q).split()
        words = [self.morph.parse(word)[0].normal_form for word in words
                 if word and word not in stopwords.words('russian')]
        heap = {}
        start = time()
        idf = {}
        for word in words:
            if not self.ind[word[0]]:
                self.ind[word[0]] = self.__load_ind_for_letter(word[0])
            idf[word] = self.__idf(word)
        for doc_id in self.doc:
            bm25 = 0.0
            for word in words:
                tf = self.__TF(word, doc_id)
                bm25 += idf[word] * (tf * (self.k1 + 1)) / (tf + self.k1 * (1 - self.b + self.b * self.__word_count(doc_id) / self.avg))
            heap[doc_id] = bm25
        print(time() - start)
        return heapq.nlargest(count, heap, key=heap.get)

    def __avgdl(self):
        self.avg = 0
        with open(os.path.join(self.data_dir, 'count'), 'rb') as f:
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
        return math.log2(self.__N() / self.__n(word))

    def __load_ind_for_letter(self, letter):
        with open(os.path.join(self.index_dir, letter + '.ind'), 'rb') as f:
            try:
                return pickle.load(f)
            except pickle.PicklingError as e:
                logging.error(str(e))

    def __load_ind(self):
        for key in self.ind:
            with open(os.path.join(self.index_dir, key + '.ind'), 'rb') as f:
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
                return i[1]
        return 0
