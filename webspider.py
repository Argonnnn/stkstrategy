import requests
import pandas as pd
import numpy as np
import datetime as dt
import tushare as ts
import os
from threading import Thread
from queue import Queue
import time
import json
import re

def run_time(func):
    def wrapper(*args,**kw):
        start = time.time()
        func(*args,**kw)
        end = time.time()
        print('running',end-start,'s')
    return wrapper

class Spider():
    def __init__(self,cols):
        self.qurl = Queue()
        self.data = list()
        self.cols = [x.lower()[-2:]+x[:6] for x in cols]
        self.exqurl = Queue()
        self.thread_num = 10

    def produce_url(self):
        base_url = "http://qt.gtimg.cn/q="
        for i in range(len(self.cols) // 900 + 1):
            self.qurl.put("http://qt.gtimg.cn/q=" + ','.join(self.cols[i*900:i*900+900]))

    def get_info(self):

        while not self.qurl.empty():
            url = self.qurl.get()
            print('crawling '+url[:30]+'...'+url[-17:])
            try:
                # url = "http://qt.gtimg.cn/q=" + ','.join(cols[:1000])
                r = requests.get(url)
                data = r.text.split(';')
                self.data.append(data[:-1])
            except Exception as e:
                print(e)
                self.exqurl.put(url)

    @run_time
    def run(self):
        self.produce_url()
        ths = []
        for _ in range(self.thread_num):
            th = Thread(target=self.get_info)
            th.start()
            ths.append(th)
        for th in ths:
            th.join()

        print('crawling finished')
        res = [(x.split('~'))[:62] for j in self.data for x in j]
        anscol = ['status', 'name', 'code', 'price', 'pre_close', 'open', 'hand1', 'outerdesk', 'innerdesk',
                  'b1price', 'b1amount', 'b2price', 'b2amount', 'b3price', 'b3amount', 'b4price', 'b4amount', 'b5price',
                  'b5amount',
                  's1price', 's1amount', 's2price', 's2amount', 's3price', 's3amount', 's4price', 's4amount', 's5price',
                  's5amount',
                  'lastdeal', 'time', 'updown', 'updownpct', 'high', 'low', 'price_hand_amount', 'hand2', 'amount',
                  'turn', 'pettm', 'x7', 'high1', 'low1', 'high2low',
                  'circulationvalue', 'totalvalue', 'pb', 'upmost', 'downmost', 'vr', 'x8', 'avgprice', 'pe_moving',
                  'pe_stay', 'x9', 'x10', 'x11',
                  'amount2', 'x12', 'x13', 'x14', 'x15']
        self.df = pd.DataFrame(res,columns=anscol).set_index('code')
        self.df.index = [y[-6:]+'.'+y[:2].upper() for y in [re.search("\w{2}\d{6}",x).group() for x in self.df.status]]


if __name__ == '__main__':
    path = r'D:\factor'
    # path = r'H:\data\factor'
    os.chdir(path + r'\basic')
    close = pd.read_pickle('close.pkl')
    cols = close.columns
    cols_judge = [x[:2] == '00' or x[:2] == '60' for x in cols]
    cols = cols[cols_judge]

    s = Spider(cols)
    s.run()
    print(s.df)

# from operator import itemgetter
#
# out = itemgetter(*cols)(aa)
