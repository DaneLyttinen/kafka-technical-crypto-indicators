import asyncio
from binance import AsyncClient, BinanceSocketManager
from binance.enums import *
from Helper import *
from config import config
producer = initProducer()

async def main():
    client = await AsyncClient.create()
    res = await asyncio.gather(
        kline_listener(producer, client, symbol="SOLBUSD"),
        quarter_poller(producer, client, symbol="SOLBUSD", time_interval=15)
    )
    await client.close_connection()

async def quarter_poller(producer, client, symbol, time_interval):
    last_val = 0
    while True:
        candles = await client.get_klines(symbol=symbol, interval=KLINE_INTERVAL_15MINUTE, limit=1)
        time = await client.get_server_time()
        minutes = dt.datetime.fromtimestamp(time['serverTime'] / 1000).minute
        #Only send if value has changed to reduce bandwidth with uneccesary information
        if (candles[0][4] != last_val):
            producer.send(topic=config['topic_3'], partition=0, value=candles)
            last_val = candles[0][4]
            print(candles)

async def kline_listener(producer, client, symbol):
    bm = BinanceSocketManager(client)
    res_count = 0
    async with bm.kline_socket(symbol=symbol, interval=KLINE_INTERVAL_1MINUTE) as stream:
        while True:
            res = await stream.recv()
            producer.send(topic=config['topic_1'],partition=0,value=res)
            producer.flush()
            res_count += 1
            #print(res)
            if res_count == 5:
                res_count = 0
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
