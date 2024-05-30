from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pandas as pd
import aiohttp
import aiomoex
from datetime import datetime, timedelta
import asyncio
import matplotlib.pyplot as plt
import json
import sqlite3
from pathlib import Path

df = pd.read_excel('etf_data.xlsx')
isins = df['Тикер']


class BaseDatabase:
    def __init__(self, filename, name='Params'):
        self.base = sqlite3.connect(filename)
        self.cursor = self.base.cursor()
        if self.base:
            print(f"{name} database connected")

    def close(self):
        self.base.close()


class UserDatabase(BaseDatabase):
    def create_database(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            name TEXT,
            tag TEXT,
            risk_profile TEXT,
            has_portfolio BOOLEAN
        )""")

        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS portfolios (
            user_id INTEGER,
            portfolio TEXT,
            FOREIGN KEY(user_id) REFERENCES users(user_id)
        )""")
        self.base.commit()

    async def save_user(self, user_id, name, tag, risk_profile, has_portfolio):
        self.cursor.execute("""
        INSERT INTO users (user_id, name, tag, risk_profile, has_portfolio)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            name=excluded.name,
            tag=excluded.tag,
            risk_profile=excluded.risk_profile,
            has_portfolio=excluded.has_portfolio
        """, (user_id, name, tag, risk_profile, has_portfolio))
        self.base.commit()

    async def save_portfolio(self, user_id, portfolio):
        pass
        # self.cursor.execute("""
        # INSERT INTO portfolios (user_id, portfolio)
        # VALUES (?, ?)
        # ON CONFLICT(user_id) DO UPDATE SET
        #     portfolio=excluded.portfolio
        # """, (user_id, portfolio))
        # self.base.commit()

    async def get_portfolio(self, user_id):
        self.cursor.execute(
            "SELECT portfolio FROM portfolios WHERE user_id = ?", (user_id,))
        return self.cursor.fetchone()

    async def parse_portfolio(self, portfolio_str):
        portfolio = json.loads(portfolio_str)
        # Здесь будет логика для получения статистики
        self.get_portfolio_statistics(portfolio)

    async def get_portfolio_statistics(self, portfolio):
        # Пустой метод, сюда вы добавите свою логику
        pass

    async def get_user(self, user_id):
        self.cursor.execute(
            "SELECT * FROM users WHERE user_id = ?", (user_id,))
        return self.cursor.fetchone()

    async def user_has_risk_profile(self, user_id):
        self.cursor.execute(
            "SELECT risk_profile FROM users WHERE user_id = ?", (user_id,))
        result = self.cursor.fetchone()
        return result is not None and result[0] is not None

    async def user_has_portfolio(self, user_id):
        self.cursor.execute(
            "SELECT has_portfolio FROM users WHERE user_id = ?", (user_id,))
        result = self.cursor.fetchone()
        return result is not None and result[0] is not None


# Создание экземпляра базы данных
user_db = UserDatabase("users.db")
user_db.create_database()


class BaseDatabase:
    def __init__(self, filename, name='Params'):
        self.base = sqlite3.connect(filename, check_same_thread=False)
        self.cursor = self.base.cursor()
        if self.base:
            print(f"{name} database connected")

    def close(self):
        self.base.close()


class MoexDatabase(BaseDatabase):
    def create_database(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS moex_data (
            isin TEXT PRIMARY KEY,
            open_prices TEXT,
            close_prices TEXT,
            volumes TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )""")
        self.base.commit()

    async def save_moex_data(self, isin, open_prices, close_prices, volumes):
        self.cursor.execute("""
        INSERT INTO moex_data (isin, open_prices, close_prices, volumes, timestamp)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(isin) DO UPDATE SET
            open_prices=excluded.open_prices,
            close_prices=excluded.close_prices,
            volumes=excluded.volumes,
            timestamp=CURRENT_TIMESTAMP
        """, (isin, open_prices, close_prices, volumes))
        self.base.commit()

    async def get_moex_stock(self, session, ticker, start='2022-01-01', end=datetime.now().strftime("%Y-%m-%d")):
        request_url = f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQIF/securities/{ticker}/candles.json?from={start}&till={end}"
        iss = aiomoex.ISSClient(session, request_url)
        data = await iss.get()
        df = pd.DataFrame(data['candles'])
        return df

    async def update_moex_data(self, isins):
        async with aiohttp.ClientSession() as session:
            tasks = [self.get_moex_stock(session, isin) for isin in isins]
            results = await asyncio.gather(*tasks)
            for (isin, df) in zip(isins, results):
                df['ISIN'] = isin

            results = [df for df in results if len(df) > 0 and
                       df['ISIN'].values[0] not in ('RU000A1013V9', 'RU000A0JTVY1', 'RU000A104172', 'RU000A0JPGC6')]

        for df in results:
            df_c = df.copy(deep=True)
            isin = df_c['ISIN'].values[0]

            price_open = df_c[['open']].rename({'open': isin}, axis=1)
            price_close = df_c[['close']].rename({'close': isin}, axis=1)
            volume = df_c[['volume']].rename({'volume': isin}, axis=1)

            # Save data to database
            await self.save_moex_data(isin, price_open.to_json(), price_close.to_json(), volume.to_json())

    async def get_cached_moex_data(self):
        self.cursor.execute(
            "SELECT isin, open_prices, close_prices, volumes FROM moex_data")
        rows = self.cursor.fetchall()
        open_prices = []
        close_prices = []
        volumes = []

        for row in rows:
            isin, open_price, close_price, volume = row
            open_prices.append(pd.read_json(open_price))
            close_prices.append(pd.read_json(close_price))
            volumes.append(pd.read_json(volume))

        return pd.concat(open_prices, axis=1), pd.concat(close_prices, axis=1), pd.concat(volumes, axis=1)


moex_db = MoexDatabase("moex_data.db")
moex_db.create_database()


async def update_data_periodically():
    while True:
        await moex_db.update_moex_data(isins)
        await asyncio.sleep(3600)  # Обновлять данные каждый час

# Запуск обновления данных в фоновом режиме
scheduler = AsyncIOScheduler()
scheduler.add_job(lambda: asyncio.run(
    moex_db.update_moex_data(isins)), 'interval', hours=1)
scheduler.start()
