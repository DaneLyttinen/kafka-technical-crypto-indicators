import collections
import statistics

from Helper import *
from config import config


def bollingerBands(market_data):
    ma = sum(market_data) / len(market_data)
    dev = statistics.stdev(market_data)
    bolu = ma + 2 * dev
    bold = ma - 2 * dev
    print("upper bound", bolu)
    print("lower bound", bold)
    return bolu, bold

print("starting consumers and producers")
consumer_1 = initConsumer(topic=config['topic_3'])
producer = initProducer()
bolu = 1000
bold = 0
market_data = collections.deque(21 * [-1], 21)
last_time_stamp = 0


while True:
    print('Consume record from topic \'{0}\' at time {1}'.format(config['topic_1'], dt.datetime.utcnow()))
    consumer_1.poll()
    records = consumeRecord(consumer_1)
    records.reverse()
    ## ensure we have a list of price data
    if records:
        # traverse records
        for record in records:
            # Only append to queue if we have a new 15 minute mark update
            if (record[0][0] != last_time_stamp):
                # Get typical price, defined as close + high + low / 3
                tp = (float(record[0][2]) + float(record[0][3]) + float(record[0][4])) / 3
                market_data.appendleft(tp)
                last_time_stamp = record[0][0]
                # Otherwise we want to update the bands depending on the new price update so we get real time updates of the bollinger bands as price updates/
            else:
                tp = (float(record[0][2]) + float(record[0][3]) + float(record[0][4])) / 3
                market_data.popleft()
                market_data.appendleft(tp)
                # Only calculate bands if list is full.
            if (-1 not in market_data):
                bolu, bold = bollingerBands(market_data)
                if (tp < bold ):
                    print("time to buy")
                if (tp > bolu):
                    print("time to sell")
                print(bolu, bold)
                print(market_data)