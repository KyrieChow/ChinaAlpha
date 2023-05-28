import pandas as pd
import numpy as np
import os
from datetime import datetime
from tqdm import tqdm

import tushare as ts

from utils import ticker_convert_tushare

pro = ts.pro_api('c0b71e0b95bff77015b4f9b6487b7188fc323ce33e306ffb272dbc6a')


if __name__ == "__main__":
    start_date = datetime(2010, 1, 1)
    end_date = datetime.today()
    path_unv = '../data/universe'
    path_raw = '../data/tushare/data_raw'
    path_save = '../data/tushare/data_clean'

    data_fields = [
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount"
    ]

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
    for ticker in tqdm(tickers):
        try:
            df = pro.daily(**{
                "ts_code": ticker_convert_tushare(ticker),
                "trade_date": "",
                "start_date": start_date.strftime('%Y%m%d'),
                "end_date": end_date.strftime('%Y%m%d'),
                "offset": "",
                "limit": ""
            }, fields=[
                "ts_code",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "change",
                "pct_chg",
                "vol",
                "amount"
            ])
            if df.shape[0] > 0:
                df.set_index('trade_date', inplace=True)
                df = df.drop(columns=['ts_code'])
                res[ticker] = df
                df.to_csv(os.path.join(path_raw, f'{ticker}.csv'))
            else:
                print(ticker, 'no data')
        except:
            print(ticker, 'failed')

    data_clean = {key: {} for key in data_fields}
    for ticker, data in res.items():
        for key in data_fields:
            data_clean[key][ticker] = data[key]

    for key in data_fields:
        temp = pd.DataFrame(data_clean[key])
        temp.index = pd.to_datetime(temp.index.astype(str))
        temp.sort_index(inplace=True)
        temp.to_csv(os.path.join(path_save, f'{key}.csv'))
