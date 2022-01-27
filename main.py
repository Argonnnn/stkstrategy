# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import pandas as pd
import webspider as wb
import uptodate_data as upda
import time
import os
import datetime as dt
import numpy as np

def updata_basic_factor(path):
    Up = upda.update(path)
    if Up.flag > 0:
        Up.update_adj()
        Up.update_daily()
        Up.update_daily_basic()

def run_time(func):
    def wrapper(*args,**kw):
        start = time.time()
        func(*args,**kw)
        end = time.time()
        print('running',end-start,'s')
    return wrapper

class pinbar():
    def __init__(self,path):
        self.path = path
        os.chdir(self.path+r'\factor_cal')
        self.case = 1
        self.today = dt.datetime.today().strftime("%Y%m%d")
        if 'pinbar.pkl' in os.listdir():
            self.case = 2
            self.pinbar = pd.read_pickle(r'pinbar.pkl')
            if self.today in self.pinbar.index.tolist():
                self.case = 3

    def update(self):
        if self.case == 3:
            print('already updated')
            return
        os.chdir(self.path + r'\basic')
        open = pd.read_pickle('open.pkl')
        close = pd.read_pickle('close.pkl')
        high = pd.read_pickle('high.pkl')
        low = pd.read_pickle('low.pkl')

        bar = abs(open - close)
        uptin = (high * 2 - open - close - bar) / 2
        downtin = (open + close - 2 * low - bar) / 2
        self.pinbar = downtin / bar
        self.pinbar.to_pickle(self.path+r'\factor_cal\pinbar.pkl')
        print('Update to '+self.pinbar.index[-1])


    @run_time
    def run_strategy(self):
        os.chdir(self.path + r'\basic')
        close = pd.read_pickle('close.pkl')
        cols = close.columns
        cols_judge = [x[:2] == '00' or x[:2] == '60' for x in cols]
        cols = cols[cols_judge]
        datelist = close.index[-60:]
        close = close.loc[datelist,cols]
        open = pd.read_pickle('open.pkl').loc[datelist,cols]
        high = pd.read_pickle('high.pkl').loc[datelist,cols]
        low = pd.read_pickle('low.pkl').loc[datelist,cols]

        s = wb.Spider(cols)
        s.run()
        # s.df.index = [x+'.SH' if x[:2]=='60' else x+'.SZ' for x in s.df.index]
        close = close.append(s.df.price._set_name(self.today)).astype(dtype = float)
        open = open.append(s.df.open._set_name(self.today)).astype(dtype = float)
        high = high.append(s.df.high._set_name(self.today)).astype(dtype = float)
        low = low.append(s.df.low._set_name(self.today)).astype(dtype = float)

        # close = close.append(s.df.price._set_name(today)).astype(dtype = float)
        # open = open.append(s.df.open._set_name(today)).astype(dtype = float)
        # high = high.append(s.df.high._set_name(today)).astype(dtype = float)
        # low = low.append(s.df.low._set_name(today)).astype(dtype = float)


        bar = abs(open - close)
        uptin = (high * 2 - open - close - bar) / 2
        downtin = (open + close - 2 * low - bar) / 2
        # select downtin/bar>2
        down2bar = downtin / bar
        up2bar = uptin/bar
        down2bar[down2bar < 2] = np.nan
        down2bar[up2bar>1] = np.nan

        self.stocks = down2bar.iloc[-1,:].dropna().sort_values()
        print(self.stocks[-round(len(self.stocks)/8):])

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    path = r'D:\factor'
    updata_basic_factor(path)
    p = pinbar(path)
    p.run_strategy()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
