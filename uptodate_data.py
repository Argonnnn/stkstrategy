#created by argon
#2021/11/18

import pandas as pd
import numpy as np
import datetime as dt
import tushare as ts
import os
pro = ts.pro_api()

class update():
    def __init__(self,path):
        self.path = path
        os.chdir(path + r'\basic')
        close = pd.read_pickle('close.pkl')
        close.index = [int(q) for q in close.index]
        startDay = max(close.index)
        endDay = dt.datetime.today().strftime('%Y%m%d')
        tradeD = pro.trade_cal(exchange='', start_date=startDay, end_date=endDay)
        tradeD = tradeD[tradeD.is_open == 1]
        tradeList = tradeD.cal_date.to_list()
        self.flag = len(tradeList)
        if len(close.loc[startDay, :].dropna()):
            tradeList.remove(str(startDay))
        self.startDay = tradeList[0]
        self.endDay = tradeList[-1]
        self.tradeList = tradeList
        self.flag = len(tradeList)

    def update_daily(self):
        if self.flag == 0:
            print('Already updated')
            return
        os.chdir(self.path + r'\basic')
        df = pd.concat([pro.daily(trade_date=i) for i in self.tradeList])
        name = ['open', 'high', 'low', 'close', 'pre_close',
                'change', 'pct_chg', 'vol', 'amount']
        for i in name:
            print(i + ' begin')
            tempD = pd.read_pickle(i + '.pkl')
            if tempD.index[-1] == self.startDay:
                tempD = tempD[:-1]
            toBeAppend = df.pivot(columns='ts_code', index='trade_date', values=i)
            toSave = pd.concat([tempD, toBeAppend], axis=0)
            toSave.index = [str(q) for q in toSave.index]
            toSave.to_pickle(i + '.pkl')

    def update_adj(self):
        if self.flag == 0:
            print('Already updated')
            return
        os.chdir(self.path + r'\basic')
        adj = pd.read_pickle('adj_factor.pkl')
        df = pd.concat([pro.adj_factor(trade_date=i) for i in self.tradeList])
        if adj.index[-1] == self.startDay:
            adj = adj[:-1]
        toBeAppend = df.pivot(columns='ts_code', index='trade_date', values='adj_factor')
        toSave = pd.concat([adj, toBeAppend], axis=0)
        toSave.index = [str(q) for q in toSave.index]
        toSave = toSave.sort_index().reset_index().drop_duplicates(subset='index', keep='first').set_index('index',drop = True)
        toSave.to_pickle('adj_factor.pkl')
        print(toSave.tail())
        print('adj finished,newest date is '+ self.endDay)

    def update_daily_basic(self):
        if self.flag == 0:
            print('Already updated')
            return
        os.chdir(self.path + r'\daily_basic')
        df = pd.concat([pro.daily_basic(trade_date=i) for i in self.tradeList])
        name = ['close', 'turnover_rate', 'turnover_rate_f', 'volume_ratio',
                'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 'dv_ratio', 'dv_ttm', 'total_share',
                'float_share', 'free_share', 'total_mv', 'circ_mv']
        for i in name:
            print(i + ' begin')
            tempD = pd.read_pickle(i + '.pkl')
            if tempD.index[-1] == self.startDay:
                tempD = tempD[:-1]
            toBeAppend = df.pivot(columns='ts_code', index='trade_date', values=i)
            toSave = pd.concat([tempD, toBeAppend], axis=0)
            toSave.index = [str(q) for q in toSave.index]
            toSave.to_pickle(i + '.pkl')




if __name__== '_main_':
    path = r'D:\factor'
    # path = r'H:\data\factor'
    Up = update(path)
    Up.update_adj()
    Up.update_daily()
    Up.update_daily_basic()


# indexlist = close.index[:2610]
# name = ['open', 'high', 'low', 'close', 'pre_close',
#                 'change', 'pct_chg', 'vol', 'amount']
# for i in name:
#     print(i + ' begin')
#     tempD = pd.read_pickle(i + '.pkl')
#     tempD.index = [str(q) for q in tempD.index]
#     toSave = tempD.loc[indexlist,:]
#     toSave.to_pickle(i + '.pkl')
#
# os.chdir(path + r'\daily_basic')
# name = ['close', 'turnover_rate', 'turnover_rate_f', 'volume_ratio',
#                 'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 'dv_ratio', 'dv_ttm', 'total_share',
#                 'float_share', 'free_share', 'total_mv', 'circ_mv']
# for i in name:
#     print(i + ' begin')
#     tempD = pd.read_pickle(i + '.pkl')
#
#     toSave = tempD.loc[indexlist,:]
#     toSave.index = [str(q) for q in toSave.index]
#     toSave.to_pickle(i + '.pkl')
