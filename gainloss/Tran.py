import math


class GDAX_CLMN:
    TradeId = 'trade id'
    Product = 'product'
    Side = 'side'
    CreatedAt = 'created at'
    Size = 'size'
    SizeUnit = 'size unit'
    Price = 'price'
    Fee = 'fee'
    Total = 'total'
    TradeUnit = 'price/fee/total unit'

    ADV_OriginalUnitPrice = 'OriginalUnitPrice'
    ADV_TradeUnitPrice = 'TradeUnitPrice'
    ADV_GainLoss = 'Gain'

    LST_Original = [
                TradeId,
                Product,
                Side,
                CreatedAt,
                Size,
                SizeUnit,
                Price,
                Fee,
                Total,
                TradeUnit
    ]


class Tran:

    def __init__(self, data):
        self.trade_id = data[GDAX_CLMN.TradeId]
        self.product = data[GDAX_CLMN.Product]
        self.side = data[GDAX_CLMN.Side]
        self.created_at = data[GDAX_CLMN.CreatedAt]
        self.size = round(math.fabs(data[GDAX_CLMN.Size]), 8)
        self.size_unit = data.get(GDAX_CLMN.SizeUnit, self.product.split('-')[0])

        self.fee = round(data.get(GDAX_CLMN.Fee, 0), 8)
        self.total = round(math.fabs(data[GDAX_CLMN.Total]), 8)

        self.price = round(data.get(GDAX_CLMN.Price, self.total / self.size), 8)

        self.price_fee_total_unit = data.get(GDAX_CLMN.TradeUnit, self.product.split('-')[1])
        self.gdax_unit_price = data.get(GDAX_CLMN.ADV_OriginalUnitPrice, None)
        self.gdax_trade_unit_price = data.get(GDAX_CLMN.ADV_TradeUnitPrice, None)
        self._buy_curr = None
        self._sell_curr = None
        self.__fill_buy_sell()

    def __fill_buy_sell(self):
        self.buy = TranUnit(self.buy_currency(),
                            self.size if self.buy_currency() == self.size_unit else self.total,
                            self.tran_usd_price())
                            # self.tran_usd_price(self.buy_currency() != self.size_unit))

        self.sell = TranUnit(self.sell_currency(),
                             self.size if self.sell_currency() == self.size_unit else self.total,
                             self.tran_usd_price())

                            # keep price the same for buy and sell for cross currency exchange
                             # self.tran_usd_price(self.sell_currency() != self.size_unit))

        # self.buy = TranUnit(self.buy_currency(),
        #                     self.size if self.buy_currency() == self.size_unit else self.total,
        #                     self.tran_usd_price(),
        #                     self.unit_usd_price() if self.buy_currency() == self.size_unit else self.gdax_trade_unit_price)
        #
        # self.sell = TranUnit(self.sell_currency(),
        #                      self.size if self.sell_currency() == self.size_unit else self.total,
        #                      self.tran_usd_price(),
        #                      self.unit_usd_price() if self.sell_currency() == self.size_unit else self.gdax_trade_unit_price)

    def buy_currency(self):
        if not self._buy_curr:
            self._buy_curr = self.product.split('-')[0 if self.is_buy_side() else 1]

        return self._buy_curr

    def sell_currency(self):
        if not self._sell_curr:
            self._sell_curr = self.product.split('-')[0 if not self.is_buy_side() else 1]

        return self._sell_curr

    def convert_fee_to_base(self, curr):
        if self.fee > 0:
            if self.buy_currency() == curr:
                self.buy.usd_total_price = self.buy.usd_total_price + self.tran_usd_fee()
                self.buy.usd_unit_price = round(self.buy.usd_total_price / self.buy.vol, 8)
            else:
                self.sell.usd_total_price = self.sell.usd_total_price - self.tran_usd_fee()
                self.sell.usd_unit_price = round(self.sell.usd_total_price / self.sell.vol, 8)
                # self.buy.usd_unit_price = round(self.buy.vol / self.buy.usd_total_price, 8)
            self.fee = 0



    def is_buy_side(self):
        return self.side == 'BUY'

    def is_usd_unit(self):
        return self.price_fee_total_unit == 'USD'

    def tran_usd_price(self, buy_price=True):
        if self.is_usd_unit():
            return self.total

        return round(self.size * self.gdax_unit_price, 8) if buy_price else \
            round(self.total * self.gdax_trade_unit_price, 8)

    def unit_usd_price(self):
        if self.is_usd_unit():
            return self.price

        return self.gdax_unit_price

    def tran_usd_fee(self, vol=None):
        vol = self.size if vol is None else vol

        if self.is_usd_unit():
            return self.fee * (vol/self.size)

        return self.fee * self.gdax_trade_unit_price * (vol/self.size)

    def convert_to_tax_tran(self, curr):
        self.convert_fee_to_base(curr)

        units = self.buy if self.buy_currency() == curr else self.sell
        return {GDAX_CLMN.TradeId: self.trade_id,
                GDAX_CLMN.Product: curr + '-USD',
                GDAX_CLMN.Side: 'BUY' if self.buy_currency() == curr else 'SELL',
                GDAX_CLMN.CreatedAt: self.created_at,
                GDAX_CLMN.Size: units.vol,
                GDAX_CLMN.Total: round(units.usd_total_price, 2 if self.is_usd_unit() else 8)}


class TranUnit:

    def __init__(self, cur, vol, total_price, usd_unit_price=None):
        self.cur = cur
        self.vol = vol
        self.usd_total_price = total_price
        self.usd_unit_price = usd_unit_price if usd_unit_price else \
            round(self.usd_total_price / self.vol, 2 if self.cur == 'USD' else 8)

    def getCost(self, vol):
        return round(self.usd_unit_price * vol, 2 if self.cur == 'USD' else 8)

    def __str__(self):
        return '{c}@{v}/{t};{p}'.format(c=self.cur, v=self.vol, t=self.usd_unit_price, p=self.usd_total_price)

