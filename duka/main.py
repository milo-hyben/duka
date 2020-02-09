#!/usr/bin/env python3.5

import argparse
from datetime import date, timedelta

from duka.app import app
from duka.core import valid_date, set_up_signals
from duka.core.utils import valid_timeframes, valid_throtteling, TimeFrame

VERSION = '0.2.1'


def main():
	parser = argparse.ArgumentParser(prog='duka', usage='%(prog)s [options]')
	parser.add_argument('-v', '--version', action='version',
						version='Version: %(prog)s-{version}'.format(version=VERSION))
	parser.add_argument('symbols', metavar='SYMBOLS', type=str, nargs='+',
						help='symbol list using format EURUSD EURGBP')
	parser.add_argument('-d', '--day', type=valid_date, help='specific day format YYYY-MM-DD (default today)',
						default=date.today() - timedelta(1))
	parser.add_argument('-s', '--startdate', type=valid_date, help='start date format YYYY-MM-DD (default today)')
	parser.add_argument('-e', '--enddate', type=valid_date, help='end date format YYYY-MM-DD (default today)')
	parser.add_argument('-t', '--throtteling', type=valid_throtteling, help='number of downloads (default 50 per 5 sec). Format: 50/5', default='none')
	parser.add_argument('-f', '--folder', type=str, help='destination folder (default .)', default='.')
	parser.add_argument('-tf', '--timeframes', type=valid_timeframes,
						help='One or multiple Time frames to export. Example: TICK,H1. Accepted values: TICK M1 M2 M5 M10 M15 M30 H1 H4',
						default=[TimeFrame.TICK])
	parser.add_argument('--header', action='store_true', help='include CSV header (default false)', default=False)
	parser.add_argument('--local-time', action='store_true', help='use local time (default GMT)', default=False)
	args = parser.parse_args()

	if args.startdate is not None:
		start = args.startdate
	else:
		start = args.day

	if args.enddate is not None:
		end = args.enddate
	else:
		end = args.day

	set_up_signals()
	print('symbols')
	print(args.symbols)
	app(args.symbols, start, end, args.throtteling, args.timeframes, args.folder, args.header, args.local_time)


if __name__ == '__main__':
	main()
