import io
import time
from functools import wraps
import requests
import datetime
import threading

import yaml
import gdax
import pandas as pd
import numpy as np

def rate_limited(max_per_second: int):
    """Rate-limits the decorated function locally, for one process."""
    lock = threading.Lock()
    min_interval = 1.0 / max_per_second

    def decorate(func):
        last_time_called = time.perf_counter()

        @wraps(func)
        def rate_limited_function(*args, **kwargs):
            lock.acquire()
            nonlocal last_time_called
            try:
                elapsed = time.perf_counter() - last_time_called
                left_to_wait = min_interval - elapsed
                if left_to_wait > 0:
                    time.sleep(left_to_wait)

                return func(*args, **kwargs)
            finally:
                last_time_called = time.perf_counter()
                lock.release()

        return rate_limited_function

    return decorate

class ReportLoader:

    STANDARD_DELAY = 0.5

    def __init__(self, passphrase, key, b64secret):
        self.gdax = gdax.AuthenticatedClient(key=key, b64secret=b64secret, passphrase=passphrase)
        self.gdax_public = gdax.PublicClient()
        # self.products = None

    @classmethod
    def from_config(cls, config_path):
        with open(config_path) as f:
            config = yaml.safe_load(f)

        return cls(passphrase=config['passphrase'],
                   key=config['key'],
                   b64secret=config['b64secret'])

    def download_reports(self, products, start_date, end_date):
        """

        :param currency:
        :param products:
        :param start_date:
        :param end_date:
        :return:

        @rtype: pd.DataFrame
        """
        report_ids = []
        for p in products:

            result = self.gdax.create_report(
                report_type="fills",
                start_date=str(start_date),
                end_date=str(end_date),
                product_id=p,
                report_format='csv')
            report_ids.append(result['id'])

        self.__doublesleep()

        report_urls = []

        while len(report_ids) > 0:

            for rid in report_ids:
                res = self.gdax.get_report(rid)
                if res['status'] == 'ready':
                    report_urls.append(res['file_url'])
                    report_ids.remove(rid)
                    self.__doublesleep()

        data_frames = []
        for url in report_urls:
            print(url)
            s = requests.get(url).content
            data_frames.append(
                pd.read_csv(io.StringIO(s.decode('utf-8')))
            )

        return pd.concat(data_frames, ignore_index=True)

    @rate_limited(1.5)
    def getHistoricalUsdVal(self, currency, date, timedelta=15):
        #https://min-api.cryptocompare.com/data/pricehistorical?fsym=ETH&tsyms=BTC,USD,EUR&ts=1518723173&e=Coinbase
        start_date = date - datetime.timedelta(seconds=timedelta)
        end_date = date + datetime.timedelta(seconds=timedelta)

        result = self.gdax_public.get_product_historic_rates(
            '{unit}-USD'.format(unit=currency),
            start=start_date,
            end=end_date,
            granularity=60)

        return round(np.mean([(row[1]+row[2])/2 for row in result]), 2)


    def __sleep(self, t=STANDARD_DELAY):
        time.sleep(t)


    def __doublesleep(self):
        self.__sleep(ReportLoader.STANDARD_DELAY * 2)


    def sleep(self):
        self.__sleep()


    def dsleep(self):
        self.__doublesleep()
