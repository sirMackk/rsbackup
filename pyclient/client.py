import asyncio
import functools
import pathlib

import aiohttp
import click

# TODO:
# - client: submit data, retrieve data, check data health
# - cli util
#   - custom ssl cert
#   - server_url
# - typehints
# - security: ssl for transport sec, authn
# - docstrings


class BackuperError(Exception):
    pass


class ServerError(BackuperError):
    pass


class Client():
    SERVER_URLMAP = {
        'list_data': 'list_data',
    }

    def __init__(self, server_url: str = 'localhost:44987') -> None:
        self.server_url = server_url

    async def submit_data(self, filepath: pathlib.Path) -> None:
        with aiohttp.ClientSession() as session:
            pass

    async def retrieve_data(self, uuid: str,
                            target_path: pathlib.Path) -> None:
        with aiohttp.ClientSesion() as session:
            pass

    async def check_data(self, uuid: str) -> None:
        with aiohttp.ClientSession() as session:
            pass

    async def list_data(self) -> None:
        # rsp = {files: [{uuid, sha256, name, lmod},]}
        async with aiohttp.ClientSession() as session:
            async with session.get(
                    f'{self.server_url}/{self.SERVER_URLMAP["list_data"]}'
            ) as rsp:
                if rsp.status != 200:
                    raise ServerError(await rsp.text())
                data_list = await rsp.json()
                if not data_list['files']:
                    print('No files!')
                for file_ in data_list['files']:
                    print('=' * 80)
                    print(f'name: {file_["name"]}')
                    print(f'last modified: {file_["lmod"]}')
                    print(f'uuid: {file_["uuid"]}')
                    print(f'sha256sum: {file_["sha256"]}')


def _run_client_fn(fn) -> None:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fn)


def common_options(func):
    @click.option('--debug/--no-debug', default=False)
    @click.option('-s', '--server-url', type=str)
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper


@click.group()
def cli() -> None:
    pass


@cli.command()
@common_options
def list_data(debug, server_url):
    """List data"""
    client = Client(server_url)
    _run_client_fn(client.list_data)


if __name__ == '__main__':
    cli()
