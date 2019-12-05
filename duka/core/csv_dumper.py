import csv
import time
from os.path import join

from .candle import Candle
from .utils import TimeFrame, stringify, Logger

TEMPLATE_FILE_NAME = "{}-{}_{:02d}_{:02d}-{}_{:02d}_{:02d}-{}.csv"


def format_float(number):
    return format(number, '.5f')


class CSVFormatter(object):
    COLUMN_TIME = 0
    COLUMN_ASK = 1
    COLUMN_BID = 2
    COLUMN_ASK_VOLUME = 3
    COLUMN_BID_VOLUME = 4


def write_tick(writer, tick):
    writer.writerow(
        {'time': tick[0],
         'ask': format_float(tick[1]),
         'bid': format_float(tick[2]),
         'ask_volume': tick[3],
         'bid_volume': tick[4]})


def write_candle(writer, candle):
    writer.writerow(
        {'time': stringify(candle.timestamp),
         'open': format_float(candle.open_price),
         'close': format_float(candle.close_price),
         'high': format_float(candle.high),
         'low': format_float(candle.low)})


class CSVDumper:
    def __init__(self, symbol, timeframes, start, end, folder, header=False):
        self.symbol = symbol
        self.timeframes = timeframes
        self.start = start
        self.end = end
        self.folder = folder
        self.include_header = header
        self.buffers = {}
        for timeframe in timeframes:
            self.buffers[timeframe] = {}

    def get_header(self, timeframe):
        if timeframe == TimeFrame.TICK:
            return ['time', 'ask', 'bid', 'ask_volume', 'bid_volume']
        return ['time', 'open', 'close', 'high', 'low']

    def append(self, day, ticks):
        for timeframe in self.timeframes:
            self.buffers[timeframe][day] = []

            previous_key = None
            current_ticks = []

            # We´re writing out the data
            for tick in ticks:
                if timeframe == TimeFrame.TICK:
                    self.buffers[timeframe][day].append(tick)
                else:
                    ts = time.mktime(tick[0].timetuple())
                    key = int(ts - (ts % timeframe))
                    if previous_key != key and previous_key is not None:
                        n = int((key - previous_key) / timeframe)
                        for i in range(0, n):
                            self.buffers[timeframe][day].append(
                                Candle(self.symbol, previous_key + i * timeframe, timeframe, current_ticks))
                        current_ticks = []
                    current_ticks.append(tick[1])
                    previous_key = key

            # If we´re doing candles the data for the last period is written here (if there is data, not on sundays)
            if timeframe != TimeFrame.TICK and len(ticks) != 0:
                self.buffers[timeframe][day].append(Candle(self.symbol, previous_key, timeframe, current_ticks))

    def dump(self):
        # For each timeseries
        for timeframe in self.timeframes:

            # The file name format
            file_name = TEMPLATE_FILE_NAME.format(self.symbol,
                                                  self.start.year, self.start.month, self.start.day,
                                                  self.end.year, self.end.month, self.end.day, timeframe)

            Logger.info("Writing {0}".format(file_name))

            with open(join(self.folder, file_name), 'w', newline='\n') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=self.get_header(timeframe))
                if self.include_header:
                    writer.writeheader()
                for day in sorted(self.buffers[timeframe].keys()):
                    for value in self.buffers[timeframe][day]:
                        if timeframe == TimeFrame.TICK:
                            write_tick(writer, value)
                        else:
                            write_candle(writer, value)

            Logger.info("{0} completed".format(file_name))
