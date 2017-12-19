from tdx.engine import Engine
import pandas as pd
from collections import OrderedDict
import numpy as np
import click
import pytz
import logging as logger
from zipline.utils.util import fillna
from . import core as bundles
from zipline.utils.calendars import get_calendar

from ..fundamental import FundamentalWriter
from os.path import join

from functools import partial
from numpy import searchsorted

logger.basicConfig(level=logger.INFO)

OHLC_RATIO = 1000
DAY_BARS_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "day",
    "id"
]

SCALED_COLUMNS = [
    "open",
    "high",
    "low",
    "close"
]


def fetch_symbols(engine, assets=None):
    if assets is not None:
        stock_list = engine.security_list
        stock_list.name = stock_list.name.str.rstrip("\x00")
        stock_list = stock_list[stock_list.code.isin(assets.symbol)]
        stock_list = stock_list[stock_list.name.isin(assets.name)]
    else:
        stock_list = engine.stock_list
    symbols = pd.DataFrame()
    symbols['symbol'] = stock_list.code
    symbols['asset_name'] = stock_list.name
    symbols['exchange'] = 'SHSZ'  # calendar name
    return symbols.reset_index()


def fetch_single_equity(engine, symbol, start=None, end=None, freq='1d'):
    df = engine.get_security_bars(symbol, freq, start, end)
    df['volume'] = df['vol'].astype(np.int32) * 100  # hands * 100 == shares

    if freq == '1d':
        df.index = df.index.normalize()  # change datetime at 15:00 to midnight
        df['id'] = int(symbol)

    if start:
        df = df[df.index >= start]
    if end:
        df = df[df.index < end]
    return df.drop(['vol', 'amount', 'code'], axis=1)


def fetch_single_split_and_dividend(engine, symbol):
    df = engine.xdxr(symbol)
    if df.empty:
        return pd.DataFrame(),pd.DataFrame()
    df = df[(df.category == 1) & (df.peigu == 0)]
    if df.empty:
        return pd.DataFrame(),pd.DataFrame()
    splits = pd.DataFrame({
        'sid': int(symbol),
        'effective_date': df.index,
        'ratio': 10 / (10 + df.songzhuangu)
    })

    dividends = pd.DataFrame({
        'sid': int(symbol),
        'ex_date': df.index,
        'amount': df.fenhong / 10,
        'record_date': pd.NaT,
        'declared_date': pd.NaT,
        'pay_date': pd.NaT
    })

    return splits, dividends


def fetch_splits_and_dividends(engine, symbols, start=None, end=None):
    all_splits = []
    all_dividends = []
    for symbol in symbols['symbol']:
        split, dividend = fetch_single_split_and_dividend(engine, symbol)
        all_splits.append(split)
        all_dividends.append(dividend)

    splits = pd.concat(all_splits)
    dividends = pd.concat(all_dividends)
    if start:
        dividends = dividends[dividends.ex_date >= start]
        splits = splits[splits.effective_date >= start]
    if end:
        dividends = dividends[dividends.ex_date < end]
        splits = splits[splits.effective_date < end]
    return splits[splits.ratio != 1], dividends[dividends.amount != 0]


def get_meta_from_bars(df):
    index = df.index
    return OrderedDict([
        ("start_date", index[0]),
        ("end_date", index[-1]),
        ("first_traded", index[0]),
        ("auto_close_date", index[-1])
    ])


def reindex_to_calendar(calendar, data, freq='1d'):
    start_session, end_session = data.index[[0, -1]]
    if not isinstance(start_session, pd.Timestamp):
        start_session = pd.Timestamp(start_session, unit='m')
        end_session = pd.Timestamp(end_session, unit='m')

    start_session = start_session.normalize()
    end_session = end_session.normalize()

    if freq == '1d':
        all_sessions = calendar.sessions_in_range(start_session, end_session).tz_localize(None)
        df = data.reindex(all_sessions, copy=False)
        df = fillna(df)
        df.id.fillna(method='pad', inplace=True)
        df.day = df.index.values.astype('datetime64[m]').astype(np.int64)
    else:
        all_sessions = calendar.minutes_for_sessions_in_range(start_session, end_session).tz_localize(None)
        data.index = data.index.tz_localize(pytz.timezone('Asia/Shanghai')).tz_convert('UTC').tz_localize(None)
        df = data.reindex(all_sessions, copy=False)
        df = fillna(df)

    return df


def tdx_bundle(assets,
               ingest_minute,  # whether to ingest minute data, default False
               overwrite,
               fundamental,     # whether to ingest fundamental data, default False
               environ,
               asset_db_writer,
               minute_bar_writer,
               daily_bar_writer,
               adjustment_writer,
               fundamental_writer,
               calendar,
               start_session,
               end_session,
               cache,
               show_progress,
               output_dir):
    eg = Engine(auto_retry=True, multithread=True, best_ip=True, thread_num=8)
    eg.connect()

    symbols = fetch_symbols(eg, assets)
    metas = []

    today = pd.to_datetime('today', utc=True)
    distance = calendar.session_distance(start_session, today)
    if ingest_minute and not overwrite and (start_session < today - pd.DateOffset(years=3)):
        minute_start = calendar.all_sessions[searchsorted(calendar.all_sessions, today - pd.DateOffset(years=3))]
        logger.warning(
            "overwrite start_session for minute bars to {}(3 years),"
            " to fetch minute data before that, please add '--overwrite True'".format(minute_start))
    else:
        minute_start = start_session

    def gen_symbols_data(symbol_map, freq='1d'):
        func = partial(fetch_single_equity, eg)
        start = start_session
        end = end_session

        if freq == '1m':
            if distance >= 100:
                func = eg.get_k_data
                start = minute_start

        for index, symbol in symbol_map.iteritems():
            data = reindex_to_calendar(
                calendar,
                func(symbol, start, end, freq),
                freq=freq,
            )
            if freq == '1d':
                metas.append(get_meta_from_bars(data))
            yield int(symbol), data

    symbol_map = symbols.symbol

    assets = set([int(s) for s in symbol_map])
    daily_bar_writer.write(gen_symbols_data(symbol_map, freq="1d"), assets=assets, show_progress=show_progress)

    if ingest_minute:
        with click.progressbar(gen_symbols_data(symbol_map, freq="1m"),
                               label="Merging minute equity files:",
                               length=len(assets),
                               item_show_func=lambda e: e if e is None else str(e[0]),
                               ) as bar:
            minute_bar_writer.write(bar, show_progress=False)

    symbols = pd.concat([symbols, pd.DataFrame(data=metas)], axis=1)
    splits, dividends = fetch_splits_and_dividends(eg, symbols, start_session, end_session)
    symbols.set_index('symbol', drop=False, inplace=True)
    asset_db_writer.write(symbols)
    adjustment_writer.write(
        splits=splits,
        dividends=dividends
    )

    if fundamental:
        logger.info("writing fundamental data:")
        try:
            fundamental_writer.write(start_session,end_session )
        except Exception as e:
            pass

    eg.exit()


def register_tdx(assets=None, minute=False, start=None, overwrite=False, fundamental=False, end=None):
    try:
        bundles.unregister('tdx')
    except bundles.UnknownBundle:
        pass
    calendar = get_calendar('SHSZ')
    if start:
        if not calendar.is_session(start):
            start = calendar.all_sessions[searchsorted(calendar.all_sessions, start)]
    bundles.register('tdx', partial(tdx_bundle, assets, minute, overwrite, fundamental), 'SHSZ', start, end, minutes_per_day=240)


bundles.register('tdx', partial(tdx_bundle, None, False, False, False), minutes_per_day=240)

if __name__ == '__main__':
    eg = Engine(auto_retry=True, multithread=True, thread_num=8)
    with eg.connect():
        symbols = fetch_symbols(eg)
        symbols = symbols[:3]
        data = []
        metas = []
        for symbol in symbols.symbol:
            data.append((int(symbol), fetch_single_equity(eg, symbol)))
            metas.append(get_meta_from_bars(data[-1][1]))
        symbols = pd.concat([symbols, pd.DataFrame(data=metas)], axis=1)
        splits, dividends = fetch_splits_and_dividends(eg, symbols)
