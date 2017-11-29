from zipline.gens.tdx_client import TdxClient
import zerorpc
import click
import logging

logging.basicConfig()


@click.command()
@click.option(
    '-c',
    '--config',
    default='config.json',
    show_default=True,
    help='The config file path.',
)
@click.option(
    '-p',
    '--port',
    default=4242,
    show_default=True,
    help='port number',
)
def server(config, port):
    """
    Start tdx server.
    :return:
    """
    s = zerorpc.Server(TdxClient(config).login())
    s.bind("tcp://0.0.0.0:{}".format(port))
    s.run()
