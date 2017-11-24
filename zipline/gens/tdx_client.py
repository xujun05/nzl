# encoding: UTF-8
import json

import zipline.lib.tdx_api as tdx_api
from io import StringIO
from .type import *
import logging
import pandas as pd
from zipline.utils.util import getJsonPath
from collections import namedtuple
import datetime


class TdxClient(object):
    setting = None
    api = None
    clientID = None

    orderID = pd.DataFrame()
    orderStrategyDict = {}

    def __init__(self, config_path=''):
        assert config_path != ''
        path = getJsonPath(config_path, __file__)
        with open(path) as f:
            self.setting = json.load(f)
        self.api = tdx_api.TdxApi()
        self.api.Open(str(self.setting["dll_path"]))

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.api.Close()

    def login(self):
        self.clientID, err = self.api.Logon(str(self.setting["ip"]),
                                            self.setting["port"],
                                            str(self.setting["version"]),
                                            self.setting["yybID"],
                                            str(self.setting["account_id"]),
                                            str(self.setting["trade_account"]),
                                            str(self.setting["trade_password"]),
                                            str(self.setting["communication_password"]))
        return self

    def get_shareholder(self, stock):
        type = self.get_stock_type(stock)
        if type == 0:
            return str(self.setting["sz_shareholder_code"])
        elif type == 1:
            return str(self.setting["sh_shareholder_code"])

    @property
    def account_id(self):
        return self.setting["account_id"]

    def orders(self):
        df, err = self.api.QueryDatas(self.clientID, [TODAY_ENTRUSTMENT])
        df = self.process_data(df)
        rt = {}
        for index, row in df.T.iteritems():
            order_id = row["委托编号"]
            mul = -1 if row["买卖标志"] == 1 else 1
            rt[order_id] = Order(
                dt=str(pd.to_datetime("today").date()) + " " + row["委托时间"],
                # TODO timezone, zerorpc can't serialize datetime
                symbol=row["证券代码"],
                name=row["证券名称"],
                status=row["状态说明"],
                price=row["委托价格"],
                amount=mul * row["委托数量"],
                order_id=row["委托编号"],
                average_cost=row["成交价格"],
                filled=mul * row["成交数量"]
            )

        return rt

    # 佣金
    # 过户费
    # 印花税
    # 经手费
    # 证管费

    def _transactions(self, start_date, end_date):
        today = pd.to_datetime('today')
        today_str = today.strftime('%Y%m%d')

        rt = {}

        today_trans = True if today_str == start_date and today_str == end_date else False
        if today_trans:
            df, err = self.api.QueryData(self.clientID, TODAY_TRANSACTION)
            df = self.process_data(df)
        else:
            df, err = self.api.QueryHistoryData(self.clientID, HISTORY_TRANSACTION, start_date, end_date)
            df = self.process_data(df)
            mask = (df["买卖标志.1"] == "证券卖出") | (df["买卖标志.1"] == "证券买入")
            df = df[mask]

        for index, row in df.T.iteritems():
            id = row["成交编号"]
            sign = -1 if row["买卖标志"] == 1 else 1
            if today_trans:
                commission = row["成交金额"] * 0.0012
                dt = str(today.date()) + " " + row["成交时间"]
            else:
                commission = row["佣金"] + row["过户费"] + row["印花税"] + row["经手费"] + row["证管费"]
                dt = str(datetime.datetime.strptime(str(row["成交日期"]), "%Y%m%d").date()) + " " + row["成交时间"],
            rt[id] = Transaction(
                id=id,
                asset=row["证券代码"],
                amount=sign * row["成交数量"],
                dt=dt,
                price=row["成交价格"],
                order_id=row["委托编号"],
                commission=commission
            )
        return rt

    def transactions(self):
        start_date = end_date = pd.to_datetime('today').strftime('%Y%m%d')
        return self._transactions(start_date, end_date)

    # return 1 if sh, 0 if sz
    @staticmethod
    def get_stock_type(self, stock):
        one = stock[0]
        if one == '5' or one == '6' or one == '9':
            return 1

        if stock.startswith("009") or stock.startswith("126") or stock.startswith("110") or stock.startswith(
                "201") or stock.startswith("202") or stock.startswith("203") or stock.startswith("204"):
            return 1

        return 0

    def process_data(self, strs, type=None):
        if not isinstance(strs, list):
            strs = [strs]
        rt = []

        for s in strs:
            try:
                rt.append(pd.read_csv(StringIO(unicode(s.decode('gbk'))), sep="\t",
                                      dtype={"证券代码": str, "证券数量": int, "可卖数量": int}))
            except Exception as e:
                pass

        if not type:  # 订单或者行情返回合并的data frame
            if type == 'order' or type == 'quote':
                if (len(rt) == 0):
                    return pd.DataFrame()
                else:
                    return pd.concat(rt)

        if len(rt) == 1:
            rt = rt[0]
        return rt

    def logoff(self):
        self.api.Logoff()

    def query_data(self, category):
        if not isinstance(category, list):
            category = [category]
        data, err = self.api.QueryDatas(self.clientID, category)
        try:
            data = self.process_data(data)
        except Exception as e:
            data = None
        return data, err

    def query_history_data(self, catetory, start_date, end_date):
        data, err = self.api.QueryHistoryData(self.clientID, catetory, start_date, end_date)
        return self.process_data(data), err

    def send_orders(self, category, price_type, shareholder, code, price, number):
        data, err = self.api.SendOrders(self.clientID, category, price_type, shareholder, code, price, number)
        if len(data) != 0:
            data = self.process_data(data, 'order')["委托编号"]
            if self.orderID.empty:
                self.orderID = data
            else:
                self.orderID.append(data)
        else:
            logging.log(logging.WARNING, err)

    def can_cancel(self):
        if not self.orderID.empty:
            data, err = self.api.QueryDatas(self.clientID, [CAN_CANCEL])
            if data[0].empty:
                self.orderID = pd.DataFrame()

    def buy_limit(self, code, number, price):
        shareholder = [self.get_shareholder(co) for co in code]
        category = [BUY] * len(code)
        price_type = [LIMIT_CHARGE] * len(code)
        data, err = self.api.SendOrders(self.clientID, category, price_type, shareholder, code, price, number)
        return self.process_data(data), err

    def sell_limit(self, code, number, price):
        shareholder = [self.get_shareholder(co) for co in code]
        category = [SELL] * len(code)
        price_type = [LIMIT_CHARGE] * len(code)
        data, err = self.api.SendOrders(self.clientID, category, price_type, shareholder, code, price, number)
        return self.process_data(data), err

    def buy_market5(self, code, number):
        shareholder = [self.get_shareholder(co) for co in code]
        category = [BUY] * len(code)
        price_type = [FIVE_LEVEL_MARKET_ORDER] * len(code)
        price = [0.] * len(code)
        data, err = self.api.SendOrders(self.clientID, category, price_type, shareholder, code, price, number)
        return self.process_data(data), err

    def sell_market5(self, code, number):
        shareholder = [self.get_shareholder(co) for co in code]
        category = [SELL] * len(code)
        price_type = [FIVE_LEVEL_MARKET_ORDER] * len(code)
        price = [0.] * len(code)
        data, err = self.api.SendOrders(self.clientID, category, price_type, shareholder, code, price, number)
        return self.process_data(data), err

    # order one
    def order(self, code, number, price, action, order_type):
        shareholder = self.get_shareholder(code)
        data, err = self.api.SendOrders(self.clientID, [action], [order_type], [shareholder], [code], [price], [number])
        return self.process_data(data), err

    ### hth 委托编号
    ### jys 交易所编号
    def cancel_orders(self, jys, hth):
        if not isinstance(hth, list):
            hth = [hth]
            jys = [jys]
        data, err = self.api.CancelOrders(jys, hth)
        return self.process_data(data), err

    def get_quotes(self, code):
        if not isinstance(code, list):
            code = [code]
        data, err = self.api.GetQuotes(self.clientID, code)
        ll = self.process_data(data)
        if len(ll) == 0:
            logging.log(logging.WARNING, err)
            return pd.DataFrame(), ""
        else:
            return pd.concat(ll).drop(
                ['国债利息', '最小交易股数', '最小买入变动价位', '最小卖出变动价位', '帐号类别', '币种', '国债标识', '涨停价格', '跌停价格', '取柜台行情', '保留信息'],
                axis=1), err

    def repay(self, amount):
        data, err = self.api.Repay(self.clientID, amount)

        return self.process_data(data), err
