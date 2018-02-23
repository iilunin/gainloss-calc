import datetime

import pandas as pd
import os

from ReportLoader import ReportLoader as rpl
from ReportProcessor import ReportProcessor as rp

PATH_TRANS_TAX = './data/results_tax/'
PATH_GL_TAX = './data/results_tax_gl/'
PATH_RESULTS = './data/results/'
PATH_GDAX_ENRICHED = './data/enriched_gdax/'

FILE_NAME_TPL = '{c}_{ds}--{de}.csv'
FILE_NAME_TPL_ALT = '{c}_{ds}--{de}_alt.csv'
FILE_NAME_TPL_TOTAL = 'TOTAL_{ds}--{de}.csv'

GL_TAX_COLUMNS = ['Description', 'Date Aquired', 'Date Sold', 'Proceeds', 'Cost', 'Gain or Loss', 'Tran DT']

CURRENCIES = {
        'BCH': ['BCH-USD', 'BCH-BTC'],
        'LTC': ['LTC-USD', 'LTC-BTC'],
        'ETH': ['ETH-USD', 'ETH-BTC'],
        'BTC': ['BTC-USD', 'ETH-BTC', 'LTC-BTC']
    }

def main():
    loader = rpl.from_config('./data/gdax_conf.yaml')
    report_processor = rp(loader)


    # start_date = datetime.date(2016, 12, 31)
    start_date = datetime.date(2017, 1, 1)
    end_date = datetime.date(2017, 12, 31)



    enrich = True

    total_gains = 0

    for cur, products in CURRENCIES.items():
        total_gains += create_gain_loss_report(report_processor, start_date, end_date, cur, products, enrich)

    print('Total gains: {gain}'.format(gain=total_gains))

    merge_tax_reports(start_date, end_date)


def create_gain_loss_report(rp, start, end, cur, products, enrich=False):

    if enrich:
        cb_converted = rp.convert_cb_to_gdax('./data/coinbase/{c}_TRX.csv'.format(c=cur), './data/coinbase/{c}_TAX.csv'.format(c=cur))

        gdax_data = rp.rl.download_reports(products, datetime.date(2016, 12, 31), end)

        gdax_data = rp.merge_reports([gdax_data, cb_converted], end)
        gdax_data = rp.enrich_gdax_rpt(gdax_data)

        gdax_data.to_csv(make_path(PATH_GDAX_ENRICHED, cur, start, end), index=False)
    else:
        gdax_data = pd.read_csv(make_path(PATH_GDAX_ENRICHED, cur, start, end))

    tax_trans = rp.convert_to_tax_transactions(gdax_data, cur)
    tax_trans.to_csv(make_path(PATH_TRANS_TAX, cur, start, end), index=False)

    (gain_loss, gain_loss_tax) = rp.get_profit_loss(gdax_data, cur, start, end)
    gain_loss.to_csv(make_path(PATH_RESULTS, cur, start, end), index=False)

    if len(gain_loss_tax) > 0:
        gain_loss_tax.to_csv(make_path(PATH_GL_TAX, cur, start, end), index=False, columns=GL_TAX_COLUMNS)

    return round(0 if len(gain_loss_tax) == 0 else gain_loss_tax['Gain or Loss'].sum(), 2)


# def gain_loss_by_tran_report(rp, start, end, cur, products):
#     trans = pd.read_csv(make_path(PATH_TRANS_TAX, cur, start, end))
#
#     (gain_loss, gain_loss_tax) = rp.get_profit_loss(trans, cur, start, end)
#
#     if len(gain_loss_tax) > 0:
#         gain_loss_tax.to_csv(make_path(PATH_GL_TAX, cur, start, end), index=False, columns=GL_TAX_COLUMNS)
#
#     gain_loss.to_csv(make_path(PATH_GL_TAX, cur, start, end, FILE_NAME_TPL_ALT), index=False)
#
#     return 0 if len(gain_loss_tax) == 0 else gain_loss_tax['Gain or Loss'].sum()

def merge_tax_reports(start, end):
    merge_reports(PATH_GL_TAX, 'Tran DT', start, end)
    merge_reports(PATH_TRANS_TAX, 'created at', start, end)

def merge_reports(path, sort_key, start, end):
    trans = pd.concat(
        [pd.read_csv(make_path(path, cur, start, end)) for cur, products in CURRENCIES.items()
         if os.path.exists(make_path(path, cur, start, end))],
        ignore_index=True)

    trans[sort_key] = pd.to_datetime(trans[sort_key])
    trans = trans.sort_values(by=[sort_key])
    trans.to_csv(make_path(path, '', start, end, FILE_NAME_TPL_TOTAL), index=False)



def make_path(f, c, ds, de, tpl=FILE_NAME_TPL):
    os.makedirs(f, exist_ok=True)
    return f + tpl.format(c=c, ds=ds.isoformat(), de=de.isoformat())


if __name__ == '__main__':
    main()
