import asyncio
import pathlib
import aiohttp
import aiohttp.client_exceptions
import aiomoex
import pandas as pd
import pathlib
import requests
import pandas as pd
from datetime import datetime, timedelta
from time import sleep
from tqdm import tqdm
import concurrent.futures
import numpy as np


async def security_finder(market, board):
    df = pd.DataFrame()
    request_url = f"https://iss.moex.com/iss/engines/stock/markets/{market}/boards/{board}/securities.json"

    async with aiohttp.ClientSession() as session:
        iss = aiomoex.ISSClient(session, request_url)
        resp = await iss.get()
        tmp = pd.DataFrame(resp["securities"])
        tmp.dropna(axis=1, how='all', inplace=True)
        df = pd.concat([df, tmp], ignore_index=True)
        # print(df.head(), "\n")
       # print(df.tail(), "\n")
        # df.info()
    df['MARKET'] = iterator['MARKET'][i]
    return df


async def main(iterator):
    df = pd.DataFrame()
    flag = 1
    for i in range(0, len(iterator), 1):
        request_url = f"https://iss.moex.com/iss/engines/stock/markets/{iterator['MARKET'][i]}/boards/{iterator['BOARDID'][i]}/securities/{iterator['SECID'][i]}/trades.json"

        async with aiohttp.ClientSession() as session:
            iss = aiomoex.ISSClient(session, request_url)
            try:
                resp = await iss.get()
                flag = 1
                # print(df.head(), "\n")
                # print(df.tail(), "\n")
                # df.info
            except aiohttp.client_exceptions.ClientConnectionError:
                flag = 0
                print(iss._url)
            if flag:
                tmp = pd.DataFrame(resp["trades"])
                tmp.dropna(axis=1, how='all', inplace=True)
                df = pd.concat([df, tmp], ignore_index=True)
            ()

    return df


till_ = datetime.today().strftime('%Y-%m-%d')
from_ = (datetime.today() - timedelta(days=2)).strftime('%Y-%m-%d')

p1 = pathlib.Path()
# p1 =  pathlib.Path(r'C:\python\repo_publisher\trades_data\tmp')
iterator = pd.read_csv(p1/'adres_boards.csv', sep=';')

data = pd.DataFrame()
for i in range(0, len(iterator), 1):
    tmp = asyncio.run(security_finder(
        iterator['MARKET'][i], iterator['BOARDID'][i]))
    tmp['MARKET'] = iterator['MARKET'][i]
    data = pd.concat([data, tmp], ignore_index=True)
data.to_csv(p1/('security_finder.csv'))

iterator2 = data[['SECID', 'BOARDID', 'MARKET']].drop_duplicates()

data2 = asyncio.run(main(iterator2))
# data2.to_csv(p1/('trades_data_' + datetime.today().strftime('%Y-%m-%d') +'.csv'))
data2 = data2.merge(data[['MARKET', 'SECID', 'LOTSIZE', 'SECNAME',
                    'CURRENCYID', 'BOARDID']], on=['SECID', 'BOARDID'])
data2.to_csv(
    p1/('trades_data_' + datetime.today().strftime('%Y-%m-%d, %H-%M-%S') + '.csv'))
