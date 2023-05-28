import numpy as np
import pandas as pd
import os
from tqdm import tqdm

from alphas import Alphas


def get_alpha(data_open, data_high, data_low, data_close, data_volume, data_returns, data_vwap):
    """

    :return: Alphas 101
    """
    output_path = '../outputs/alpha101'
    al = Alphas(data_open, data_high, data_low, data_close,
                data_volume, data_returns, data_vwap)
    for alpha in tqdm(al.__methods__()):
        try:
            fun = eval("al."+alpha)
            factor = fun()
            factor.to_csv(os.path.join(output_path, f'{alpha}.csv'))
        except:
            print("Error in ", alpha)


def load_data(is_china=True):
    """
    TuShare data
    :return:
    """
    idx_path = '../data/tushare/data_clean'
    data_fields = [
        "open",
        "high",
        "low",
        "close",
        "vol",
        "amount"
    ]
    data = {}
    for key in data_fields:
        data[key] = pd.read_csv(os.path.join(idx_path, f'{key}.csv'), index_col=0).astype(float)
        data[key].index = pd.to_datetime(data[key].index)

    cols, idx = data['open'].columns, data['open'].index
    for key in data_fields:
        data[key] = data[key].reindex(index=idx, columns=cols).fillna(method='ffill')
    data['returns'] = data['close'].pct_change()
    if is_china:
        data['returns'] = np.clip(data['returns'], a_min=-0.1, a_max=0.1)

    data['vwap'] = data['amount'].div(data['vol'])

    return (data['open'], data['high'], data['low'], data['close'],
            data['vol'], data['returns'], data['vwap'])


if __name__ == "__main__":
    data_open, data_high, data_low, data_close, data_volume, data_returns, data_vwap = load_data()
    get_alpha(data_open, data_high, data_low, data_close, data_volume, data_returns, data_vwap)
