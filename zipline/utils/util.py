# encoding: UTF-8


import os
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP

# JSON配置文件路径
jsonPathDict = {}


def getJsonPath(name, moduleFile):
    """
    获取JSON配置文件的路径：
    1. 优先从当前工作目录查找JSON文件
    2. 若无法找到则前往模块所在目录查找
    """
    currentFolder = os.getcwd()
    currentJsonPath = os.path.join(currentFolder, name)
    if os.path.isfile(currentJsonPath):
        jsonPathDict[name] = currentJsonPath
        return currentJsonPath

    moduleFolder = os.path.abspath(os.path.dirname(moduleFile))
    moduleJsonPath = os.path.join(moduleFolder, '.', name)
    jsonPathDict[name] = moduleJsonPath
    return moduleJsonPath


### return 1 if sh, 0 if sz
def get_stock_type(stock):
    one = stock[0]
    if one == '5' or one == '6' or one == '9':
        return 1

    if stock.startswith("009") or stock.startswith("126") or stock.startswith("110") or stock.startswith(
            "201") or stock.startswith("202") or stock.startswith("203") or stock.startswith("204"):
        return 1

    return 0


def precise_round(num):
    return float(Decimal(str(num)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))


def fillna(df):
    mask = pd.isnull(df.close)
    df.close.fillna(method='pad', inplace=True)
    df.volume.fillna(0, inplace=True)
    df.loc[mask, ["high", "low", "open"]] = df.loc[mask, "close"]
    return df
