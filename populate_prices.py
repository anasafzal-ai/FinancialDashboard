import configuration 
import json
import requests
import datetime, time
import aiohttp, asyncpg, asyncio
import traceback

async def write_to_db(connection, params):
    await connection.copy_records_to_table('stock_price', records=params)


async def get_price(pool, stock_id, url):
    try: 
        async with pool.acquire() as connection:
            async with aiohttp.ClientSession() as session:
                async with session.get(url=url) as response:
                    resp = await response.read()
                    response = json.loads(resp)
                    empty_list = []
                    all_dates_string = sorted([(bar) for bar in response['Time Series (5min)']])
                    for date in all_dates_string:
                        key_vals = (datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S"),) + (tuple([float(i) for i in list(response.get('Time Series (5min)').get(date).values())]))
                        empty_list.append((stock_id,) + key_vals)
                    params = empty_list
                    await write_to_db(connection, params)

    except Exception as e:
        print(e)


async def get_prices(pool, symbol_urls):
    try:
        # schedule aiohttp requests to run concurrently for all symbols
        ret = await asyncio.gather(*[get_price(pool, stock_id, symbol_urls[stock_id]) for stock_id in symbol_urls])
        print("Finalized all. Returned  list of {} outputs.".format(len(ret)))
    except Exception as e:
        print(e)


async def get_stocks():
    # create database connection pool
    pool = await asyncpg.create_pool(user=configuration.DB_USER, password=configuration.DB_PASS, database=configuration.DB_NAME, host=configuration.DB_HOST, command_timeout=60)
    
    # get a connection
    async with pool.acquire() as connection:
        stocks = await connection.fetch("SELECT * FROM stock WHERE id IN (SELECT holding_id FROM etf_holding) ORDER BY id")

        symbol_urls = {}
        for stock in stocks:
            symbol_urls[stock['id']] = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={stock['symbol']}&interval=5min&outputsize=full&apikey={configuration.API_KEY}"

    await get_prices(pool, symbol_urls)


start = time.time()

asyncio.run(get_stocks())

end = time.time()

print("Took {} seconds.".format(end - start))
