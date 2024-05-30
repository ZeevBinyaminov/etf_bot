import asyncio
from datetime import datetime, timedelta
import aiomoex
import aiohttp
import pandas as pd
import requests
import apimoex

# Read the Excel file and get the ISINs
market = pd.read_excel('etf_market.xlsx')
df = pd.read_excel('etf_data.xlsx')
isins = df['Тикер']
etfs = []
today = datetime.now().date()


async def get_moex_stock(session, ticker, start='2022-01-01', end=datetime.now().strftime("%Y-%m-%d")):
    request_url = f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQIF/securities/{ticker}/candles.json?from={start}&till={end}"

    iss = aiomoex.ISSClient(session, request_url)
    data = await iss.get(start)
    df = pd.DataFrame(data['candles'])
    # Perform any additional data processing here if needed
    return df


async def main():
    async with aiohttp.ClientSession() as session:
        tasks = [get_moex_stock(session, isin) for isin in isins]
        results = await asyncio.gather(*tasks)
        for (isin, df) in zip(isins, results):
            df['ISIN'] = isin

        results = [df for df in results if len(df) > 0 and
                   df['ISIN'].values[0] not in ('RU000A1013V9', 'RU000A0JTVY1', 'RU000A104172', 'RU000A0JPGC6')]

    open_prices = []
    close_prices = []
    volumes = []

    for df in results:
        df_c = df.copy(deep=True)
        isin = df_c['ISIN'].values[0]

        price = df_c[['open']].rename({'open': isin}, axis=1)
        open_prices.append(price)

        price = df_c[['close']].rename({'close': isin}, axis=1)
        close_prices.append(price)

        volume = df_c[['volume']].rename({'volume': isin}, axis=1)
        volumes.append(volume)

    return pd.concat(open_prices), pd.concat(close_prices), pd.concat(volumes)
