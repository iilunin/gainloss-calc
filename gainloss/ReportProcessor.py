import io
import os

import pandas as pd
import numpy as np
from Tran import Tran, TranUnit, GDAX_CLMN
import logging
import datetime

logging.basicConfig(level=logging.INFO)

class ReportProcessor:
    CLM_Timestamp = 'Timestamp'
    CLM_CoinbaseID = 'Coinbase ID'
    CLM_TransferTotal = 'Transfer Total'
    CLM_TransferFee = 'Transfer Fee'
    CLM_Amount = 'Amount'
    CLM_BitcoinHash = 'Bitcoin Hash'
    CLM_GainLoss = 'Gain'
    CLM_Currency = 'Currency'

    def __init__(self, report_loader=None):
        self.rl = report_loader


    def convert_cb_to_gdax(self, cb_tran, cb_buys_sells, external_transfer_as_sell=True):
        clean_tran_csv = self.get_csv_lines(cb_tran, 4)
        clmns = clean_tran_csv[0].split(',')
        clmns[-1] = ReportProcessor.CLM_BitcoinHash
        clmns[-2] = ReportProcessor.CLM_CoinbaseID
        clean_tran_csv[0] = ','.join(clmns)

        trxs = pd.read_csv(io.StringIO(os.linesep.join(clean_tran_csv)))

        clean_buys_sells = self.get_csv_lines(cb_buys_sells, starts_with='BUYS')
        buys_sells = pd.read_csv(io.StringIO(os.linesep.join(clean_buys_sells)))

        gdax_df = pd.DataFrame(columns=GDAX_CLMN.LST_Original)

        for index, row in trxs.iterrows():
            buy_tran = row[ReportProcessor.CLM_Amount] > 0

            if np.isnan(row[ReportProcessor.CLM_TransferTotal]):  # incoming or outcoming tran

                trx_id = row[ReportProcessor.CLM_CoinbaseID]

                # check approx cost of incoming currency from external wallet
                tran_row = buys_sells.loc[buys_sells['Received Transaction ID' if buy_tran else 'Sent Transaction ID'] == trx_id].iloc[0]

                if buy_tran:
                    if tran_row['Received Description'].lower() == 'received from gdax': # skipping transfers
                        continue

                    if isinstance(row[ReportProcessor.CLM_BitcoinHash], str) and row[ReportProcessor.CLM_BitcoinHash]:
                        price_per_coin = tran_row[
                            'Received Price Per Coin (USD)']
                        row[ReportProcessor.CLM_TransferTotal] = \
                            round(price_per_coin * trxs.loc[index, ReportProcessor.CLM_Amount], 8)
                        row[ReportProcessor.CLM_TransferFee] = 0

                else:  # if sell tran
                    if tran_row['Sent Description'].lower() == 'sent to gdax':
                        continue
                    if not external_transfer_as_sell:   # consider that we still have crypto which was sent to ext addr
                        continue
                    else:                               # consider we sold crypto sent to ext addr
                        row[ReportProcessor.CLM_TransferTotal] = tran_row['Sent Total (USD)']
                        row[ReportProcessor.CLM_TransferFee] = 0

            total = row[ReportProcessor.CLM_TransferTotal] - row[ReportProcessor.CLM_TransferFee]
            price = round(total / row[ReportProcessor.CLM_Amount], 2)

            row_val = [[
                row[ReportProcessor.CLM_CoinbaseID],  # -> trade id
                row[ReportProcessor.CLM_Currency] + '-USD',  # -> Product,
                'BUY' if buy_tran else 'SELL',  # -> Side,
                row[ReportProcessor.CLM_Timestamp],  # -> CreatedAt,
                abs(row[ReportProcessor.CLM_Amount]),  # -> Size,
                row[ReportProcessor.CLM_Currency],  # -> SizeUnit,
                abs(price),  # -> Price,
                row[ReportProcessor.CLM_TransferFee],  # -> Fee,
                abs(total) if buy_tran else -abs(total),  # -> Total,
                'USD'  # -> TradeUnit,
            ]]
            gdax_row = pd.DataFrame(data=row_val, columns=GDAX_CLMN.LST_Original)
            gdax_df = gdax_df.append(gdax_row, ignore_index=True)


        return gdax_df

    def get_csv_lines(self, file, skip=0, starts_with=None):
        with open(file, 'r') as csv_file:
            if not starts_with:
                return csv_file.readlines()[skip:]
            else:
                lines = csv_file.readlines()
                for i in range(0, len(lines)):
                    if lines[i].startswith(starts_with):
                        return lines[i+1:]

    def merge_reports(self, reports, end_date=None):
        rpt = pd.concat(reports, ignore_index=True)
        rpt[GDAX_CLMN.CreatedAt] = pd.to_datetime(rpt[GDAX_CLMN.CreatedAt])
        return rpt[rpt[GDAX_CLMN.CreatedAt] <= end_date] if end_date else rpt

    def convert_to_tax_transactions(self, rpt, curr):
        return pd.DataFrame([Tran(row).convert_to_tax_tran(curr) for index, row in rpt.iterrows()])

    def create_tax_gainloss_row(self, description, date_aquired, date_sold, sales_price, cost):
        tfmt = '%m/%d/%Y'
        return {
            'Description': description,
            'Date Aquired': date_aquired.strftime(tfmt) if isinstance(date_aquired, datetime.date) else date_aquired,
            'Date Sold': date_sold.strftime(tfmt) if isinstance(date_sold, datetime.date) else date_sold,
            'Proceeds': round(sales_price, 2),
            'Cost': round(cost, 2),
            'Gain or Loss': round(sales_price - cost, 2),
            'Tran DT': date_sold
        }


    def enrich_gdax_rpt(self, rpt):
        """

        :param rpt:
        :return:

        @rtype: pd.DataFrame
        """
        rpt[GDAX_CLMN.CreatedAt] = pd.to_datetime(rpt[GDAX_CLMN.CreatedAt])
        rpt = rpt.sort_values(by=GDAX_CLMN.CreatedAt)

        for index, row in rpt.iterrows():
            if row[GDAX_CLMN.TradeUnit] == 'USD':
                continue

            rpt.loc[index, GDAX_CLMN.ADV_TradeUnitPrice] = \
                self.rl.getHistoricalUsdVal(row[GDAX_CLMN.TradeUnit], row[GDAX_CLMN.CreatedAt])
            # self.rl.dsleep()

            rpt.loc[index, GDAX_CLMN.ADV_OriginalUnitPrice] = \
                self.rl.getHistoricalUsdVal(row[GDAX_CLMN.SizeUnit], row[GDAX_CLMN.CreatedAt])
            # self.rl.dsleep()

        return rpt

    def get_profit_loss(self, rpt, currency, start, end, fifo=True, tax_gainloss=True):
        """

        :param rpt:
        :type rpt: pd.DataFrame
        :param currency:
        :param start:
        :param end:
        :param fifo:
        :return:
        :rtype: pd.DataFrame
        """

        rpt = rpt.copy()

        rpt[GDAX_CLMN.CreatedAt] = pd.to_datetime(rpt[GDAX_CLMN.CreatedAt])
        rpt[GDAX_CLMN.ADV_GainLoss] = np.nan
        rpt['info'] = ''

        # use only dataset within date range (end date). We'll need to process from the very beginning
        rpt = rpt[(rpt[GDAX_CLMN.CreatedAt] <= end)]

        transactions = []
        for index, row in rpt.iterrows():
            t = Tran(row)
            t.convert_fee_to_base(currency)
            transactions.append((t, index))

        buys = []
        gain_loss_tax_list = [] if tax_gainloss else None

        for t, row_ix in transactions:
            if t.sell_currency() != currency:
                buys.append(t)
                continue

            sell_amount = t.sell.vol  # always positive

            logging.info('Remaining balance: %s', round(sum([t.buy.vol for t in buys]), 8))
            logging.info('Selling vol: %s', sell_amount)

            info = ''
            buy_cost = 0
            buy_fee = 0

            for prev_buy in (buys[:] if fifo else buys[::-1]):

                # make sure we got 'buy' transaction for this currency and it still has volumes left
                if prev_buy.buy_currency() != currency or prev_buy.buy.vol <= 0:
                    continue

                if sell_amount - prev_buy.buy.vol >= 0:
                    sell_amount = round(sell_amount - prev_buy.buy.vol, 8)

                    if gain_loss_tax_list is not None:
                        self.add_tax_gainloss_row(gain_loss_tax_list, currency, prev_buy, t, prev_buy.buy.vol)

                    buy_cost += prev_buy.buy.usd_total_price
                    buy_fee += prev_buy.tran_usd_fee()

                    info += '{a}@{p}/{t},fee:{f};'.format(
                        a=prev_buy.buy.vol,
                        p=prev_buy.buy.usd_unit_price,
                        t=prev_buy.buy.usd_total_price,
                        f=prev_buy.tran_usd_fee())

                    prev_buy.buy.vol = 0
                    prev_buy.buy.usd_total_price = 0

                    buys.remove(prev_buy)

                    if sell_amount == 0:
                        break
                else:
                    prise_per_coin = prev_buy.buy.usd_unit_price

                    total_for_partial_tran = round(sell_amount * prise_per_coin, 8)

                    buy_cost += total_for_partial_tran

                    partial_fee = 0

                    if gain_loss_tax_list is not None:
                        self.add_tax_gainloss_row(gain_loss_tax_list, currency, prev_buy, t, sell_amount)

                    if prev_buy.tran_usd_fee() > 0:  # apply just a part of the fee

                        partial_sell_ratio = sell_amount / prev_buy.buy.vol
                        partial_fee = round(partial_sell_ratio * prev_buy.tran_usd_fee(), 8)
                        buy_fee += partial_fee

                        partial_fee_original_units = round(partial_sell_ratio * prev_buy.fee, 8)
                        # trxs.loc[pos_index, CLM_TransferFee] = partial_fee_original_units
                        prev_buy.fee = round(prev_buy.fee - partial_fee_original_units, 8)

                    prev_buy.buy.vol = round(prev_buy.buy.vol - sell_amount, 8)
                    prev_buy.buy.usd_total_price = round(prev_buy.buy.vol * prise_per_coin, 2)

                    info += '{a}@{p}/{t},fee:{f};'.format(
                        a=sell_amount,
                        p=prise_per_coin,
                        t=total_for_partial_tran,
                        f=partial_fee)

                    break

            sale_fee = t.tran_usd_fee()
            rpt.loc[row_ix, GDAX_CLMN.ADV_GainLoss] = round(t.sell.usd_total_price - buy_cost - buy_fee - sale_fee, 2)

            if sale_fee > 0:
                info += ' sale_fee:{sf}'.format(sf=sale_fee)

            rpt.loc[row_ix, 'info'] = info


        return (rpt[(rpt[GDAX_CLMN.CreatedAt] >= start) & (rpt[GDAX_CLMN.CreatedAt] <= end) & ~np.isnan(rpt[GDAX_CLMN.ADV_GainLoss])],
                pd.DataFrame([item for item in gain_loss_tax_list if item['Proceeds'] != 0]))

    def add_tax_gainloss_row(self, gain_loss_tran_list, currency, buy_tran, sell_tran, sell_amount):
        gain_loss_tran_list.append(
            self.create_tax_gainloss_row(
                '{vol} {cur}'.format(vol=sell_amount, cur=currency),
                buy_tran.created_at,
                sell_tran.created_at,
                sell_tran.sell.getCost(sell_amount) + sell_tran.tran_usd_fee(sell_amount),
                buy_tran.buy.getCost(sell_amount) - buy_tran.tran_usd_fee(sell_amount))
        )



