import struct
from datetime import timedelta, datetime
from dateutil import tz
from lzma import LZMADecompressor, LZMAError, FORMAT_AUTO
from .utils import is_dst


def decompress_lzma(data):
    results = []
    len(data)
    while True:
        decomp = LZMADecompressor(FORMAT_AUTO, None, None)
        try:
            res = decomp.decompress(data)
        except LZMAError:
            if results:
                break  # Leftover data is not a valid LZMA/XZ stream; ignore it.
            else:
                raise  # Error on the first iteration; bail out.
        results.append(res)
        data = decomp.unused_data
        if not data:
            break
        if not decomp.eof:
            raise LZMAError("Compressed data ended before the end-of-stream marker was reached")
    return b"".join(results)


def tokenize(buffer):
    token_size = 20
    size = int(len(buffer) / token_size)
    tokens = []
    for i in range(0, size):
        tokens.append(struct.unpack('!IIIff', buffer[i * token_size: (i + 1) * token_size]))
    return tokens


def normalize(symbol, day, local_time, hour, ticks):
    def norm(time, ask, bid, volume_ask, volume_bid):
        #date.replace(tzinfo=datetime.tzinfo("UTC"))
        date = datetime(day.year, day.month, day.day, hour) + timedelta(milliseconds=time)

        if local_time:
            date.replace(tzinfo=tz.tzlocal())
        else:
            date.replace(tzinfo=tz.UTC)

        point = 100000
        #print(symbol.upper())
        if symbol.upper() in ['USDRUB', 'XAGUSD', 'XAUUSD'] or 'IDX' in symbol.upper():
            point = 1000

        return date, ask / point, bid / point, round(volume_ask * 1000000), round(volume_bid * 1000000)

    return list(map(lambda x: norm(*x), ticks))


def decompress(symbol, day, local_time, buffer_map):
    ticks = []
    for hour, buffer_value in buffer_map.items():
        compressed_buffer = buffer_value.getbuffer()
        if compressed_buffer.nbytes != 0:
            ticks.extend(
                normalize(
                    symbol, day, local_time, hour,
                    tokenize(decompress_lzma(compressed_buffer))
                )
            )

    return ticks
