import asyncio
import datetime
import threading
import time
from functools import reduce
from io import BytesIO, DEFAULT_BUFFER_SIZE
from asyncio_throttle import Throttler

import requests
import requests_cache

from ..core.utils import Logger, is_dst
#https://datafeed.dukascopy.com/datafeed/SPXUSD/2019/11/03/00h_ticks.bi5
#https://datafeed.dukascopy.com/datafeed/SPXUSD/2019/10/19/00h_ticks.bi5
#URL = 'https://datafeed.dukascopy.com/datafeed/JPNIDXJPY/2019/10/BID_candles_hour_1.bi5'
URL = 'https://datafeed.dukascopy.com/datafeed/{currency}/{year}/{month:02d}/BID_candles_hour_1.bi5'
URL = "https://datafeed.dukascopy.com/datafeed/{currency}/{year}/{month:02d}/{day:02d}/{hour:02d}h_ticks.bi5"
cookies = {'_ga': 'GA1.2.1783322365.1573431135', '_fbp': 'fb.1.1573431139752.1065100632', 'visitor_id764243': '26287959', 'visitor_id764243-hash': '92a6412b696a2f2611d1bb3cdce6c8d2c02c150f360a472960e9ff3c95eb5a69b4aa57b17e759813d2cfe6e2917324638d5539a1', 'lightbox_market_brief_count': '2', 'lightbox_market_brief_last_shown': '1573525028893', 'shopping_cart': '%7B%7D', 'hedgeye_user_name': 'Thomas+Furch', 'customer_type': 'IndividualUser', 'sailthru_hid': '4b396174c23a1cc08705833ceeb236225dcaacec24c17c6ff7f455c32a296f2cfe84dc0964b2a0c58d74e7a3', '_gid': 'GA1.2.818576277.1574859050', '_gali': 'se-be-login-submit', '_hedgeye_session': '97001f984caa95ed38c67b23042b577e'}
agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'
referer = 'https://freeserv.dukascopy.com/2.0/?path=historical_data_feed/index&header=false&availableInstruments=l%3A&width=100%25&height=600&adv=popup&lang=en'
encoding = 'gzip, deflate, br',
accept = '*/*'
language = 'en-US,en;q=0.9,de;q=0.8'
origin = 'https://freeserv.dukascopy.com'
#"accept-encoding": encoding,
headers = {'user-agent': agent, 'referer': referer,  'accept-language': language, 'accept': accept, 'origin': origin, 'sec-fetch-mode': 'cors', 'sec-fetch-site': 'same-site'}

#session = requests_cache.CachedSession(cache_name='cache', backend='sqlite', expire_after=expire_after)
requests_cache.install_cache('duca_cache', backend='sqlite', expire_after=None)


throttler = None
#throttler = Throttler(rate_limit=5)




ATTEMPTS = 5
#cachedCount = 0

async def get(url):
	global throttler #, cachedCount
	print(url)
	loop = asyncio.get_event_loop()
	buffer = BytesIO()
	id = url[35:].replace('/', " ")
	start = time.time()
	Logger.info("Fetching {0}".format(id))
	for i in range(ATTEMPTS):
		try:
			res = await loop.run_in_executor(None, lambda: requests.get(url, stream=True))
			#print('res.status_code', res.status_code)
			#print('res.headers', res.headers)
			if res.from_cache is True:
				print('cached')
			else:
				if throttler:
					async with throttler:
						Logger.debug(time.time(), 'Rate Limit')
				#cachedCount = cachedCount + 1
			#print('res.from_cache')
			#print(res.from_cache)
			if res.status_code == 200:
				for chunk in res.iter_content(DEFAULT_BUFFER_SIZE):
					buffer.write(chunk)
				Logger.info("Fetched {0} completed in {1}s".format(id, time.time() - start))
				if len(buffer.getbuffer()) <= 0:
					Logger.info("Buffer for {0} is empty ".format(id))
				print('done.')
				return buffer.getbuffer()
			else:
				Logger.warn("Request to {0} failed with error code : {1} ".format(url, str(res.status_code)))
		except Exception as e:
			print('***Exception***', str(e))
			Logger.warn("Request {0} failed with exception : {1}".format(id, str(e)))
			time.sleep(0.5 * i)

	raise Exception("Request failed for {0} after {1} attempts".format(url, ATTEMPTS))


def create_tasks(symbol, day, throtteling):
	global throttler
	#print('throtteling?')
	#print(throtteling)
	#print('throttler?')
	#print(throttler)
	if throttler == None and throtteling != None:
		#print('established throttler')
		#print(throtteling[0])
		#print(throtteling[1])
		throttler = Throttler(rate_limit=throtteling[0], period=throtteling[1])

	start = 0

	if is_dst(day):
		start = 1

	url_info = {
		'currency': symbol,
		'year': day.year,
		'month': day.month - 1,
		'day': day.day
	}
	tasks = [asyncio.ensure_future(get(URL.format(**url_info, hour=i))) for i in range(0, 24)] #24

	# if is_dst(day):
	#     next_day = day + datetime.timedelta(days=1)
	#     url_info = {
	#         'currency': symbol,
	#         'year': next_day.year,
	#         'month': next_day.month - 1,
	#         'day': next_day.day
	#     }
	#     tasks.append(asyncio.ensure_future(get(URL.format(**url_info, hour=0))))
	return tasks


def fetch_day(symbol, day, throtteling):
	local_data = threading.local()
	loop = getattr(local_data, 'loop', asyncio.new_event_loop())
	asyncio.set_event_loop(loop)
	loop = asyncio.get_event_loop()
	tasks = create_tasks(symbol, day, throtteling)
	loop.run_until_complete(asyncio.wait(tasks))

	def add(acc, task):
		acc.write(task.result())
		return acc

	return reduce(add, tasks, BytesIO()).getbuffer()
