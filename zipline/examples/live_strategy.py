# coding=utf-8

from zipline.api import order, record, symbol
import platform

def initialize(context):
    context.smb = symbol('601118')


def handle_data(context, data):
    can_trade = data.can_trade(context.smb)
    hist = data.history(symbol('601118'), bar_count=20, frequency='1m', fields='open')
    print(hist)


if __name__ == '__main__':
    from zipline.utils.cli import Date
    from zipline.utils.run_algo import run_algorithm
    from zipline.gens.brokers.tdx_broker import TdxBroker
    import pandas as pd
    import os

    if platform.architecture()[0] == '32bit':
        client_uri = 'config.json'
    else:
        client_uri = "tcp://127.0.0.1:4242"
    broker = TdxBroker(client_uri)
    if not os.path.exists('tmp'):
        os.mkdir('tmp')
    realtime_bar_target = 'tmp/real-bar-{}'.format(str(pd.to_datetime('today').date()))
    state_filename = 'tmp/live-state'

    start = Date(tz='utc', as_timestamp=True).parser('2017-03-01')

    end = Date(tz='utc', as_timestamp=True).parser('2017-11-01')

    run_algorithm(start, end, initialize, 10e6, handle_data=handle_data, bundle='tdx',
                  trading_calendar='SHSZ', data_frequency="minute", output='out.pickle',
                  broker=broker, state_filename=state_filename, realtime_bar_target=realtime_bar_target)
