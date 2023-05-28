import pandas as pd
import numpy as np
import os
from datetime import datetime

import yfinance as yf

from utils import ticker_convert


if __name__ == "__main__":
    start_date = datetime(2010, 1, 1)
    end_date = datetime.today()
    path_unv = '../data/universe'
    path_raw = '../data/yfinance/data_raw'
    path_save = '../data/yfinance/data_clean'

    tickers = []
    files = os.listdir(path_unv)
    for file in files:
        try:
            df = pd.read_excel(os.path.join(path_unv, file))
            tickers += df['Bloomberg'].tolist()
        except:
            pass

    tickers = np.unique(tickers)
    res = {}
    for ticker in tickers:
        ticker_temp = ticker_convert(ticker)
        data = yf.download(ticker_temp, start=start_date, end=end_date)
        if data.shape[0] > 0:
            data.to_csv(os.path.join(path_raw, f'{ticker}.csv'))
            res[ticker] = data

    data_open = {}
    data_high = {}
    data_low = {}
    data_close = {}
    data_adj_close = {}
    data_volume = {}

    for ticker, data in res.items():
        data_open[ticker] = data['Open']
        data_high[ticker] = data['High']
        data_low[ticker] = data['Low']
        data_close[ticker] = data['Close']
        data_adj_close[ticker] = data['Adj Close']
        data_volume[ticker] = data['Volume']

    pd.DataFrame(data_open).to_csv(os.path.join(path_save, 'open.csv'))
    pd.DataFrame(data_high).to_csv(os.path.join(path_save, 'high.csv'))
    pd.DataFrame(data_low).to_csv(os.path.join(path_save, 'low.csv'))
    pd.DataFrame(data_close).to_csv(os.path.join(path_save, 'close.csv'))
    pd.DataFrame(data_adj_close).to_csv(os.path.join(path_save, 'adjClose.csv'))
    pd.DataFrame(data_volume).to_csv(os.path.join(path_save, 'volume.csv'))
