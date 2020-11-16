import asyncio
import functools
import hashlib
import logging
import pathlib
import typing

import aiohttp
import click

# TODO:
# - typehints
# - security: ssl, auth?
# - docstrings


class BackuperError(Exception):
    pass


class ServerError(BackuperError):
    pass


class ClientError(BackuperError):
    pass


class Client():
    SERVER_URLMAP = {
        'list_data': 'list_data',
        'check_data': 'check_data',
        'submit_data': 'submit_data',
        'retrieve_data': 'retrieve_data',
    }

    def __init__(self,
                 timeout: int = 5,
                 server_url: str = 'http://localhost:44987') -> None:
        self.server_url = self._format_server_url(server_url)
        self.timeout = aiohttp.ClientTimeout(total=float(timeout))

    def _format_server_url(self, url: str) -> str:
        if url.startswith('http://') or url.startswith('https://'):
            return url
        else:
            return f'http://{url}'

    def _sha256(self, file_: typing.BinaryIO) -> str:
        file_.seek(0)
        block_size = 65536
        hasher = hashlib.sha256()
        buf = file_.read(block_size)
        while len(buf) > 0:
            hasher.update(buf)
            buf = file_.read(block_size)
        file_.seek(0)
        return hasher.hexdigest()

    async def submit_data(self, fname: str, filepath: pathlib.Path) -> None:
        # rsp = {submitted: {sha256}}
        if not filepath.exists():
            raise ClientError(f"{filepath} does not exist!")
        if not filepath.is_file():
            raise ClientError(f"{filepath} is not a file!")

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            with open(filepath, 'rb') as f:
                sha256_digest = self._sha256(f)
                print('=' * 80)
                print(f'Uploading file: {filepath}')
                print(f'sha256: {sha256_digest}')
                async with session.post(
                    f'{self.server_url}/{self.SERVER_URLMAP["submit_data"]}',
                    data=f
                ) as rsp:
                    if rsp.status != 200:
                        raise ServerError(await rsp.text())
                    data_submit = (await rsp.json())['submitted']
                    print('=' * 80)
                    # TODO what if digests dont match?
                    print('Status: SUCCESS')
                    print(f'sha256: {data_submit["sha256"]}')
                    print(
                        f'sha256 match: {sha256_digest == data_submit["sha256"]}'
                    )

    async def _save_rsp_to_file(self, rsp: aiohttp.ClientResponse,
                                path: pathlib.Path) -> None:
        chunk_size = 66560
        with open(path, 'wb') as f:
            while True:
                chunk = await rsp.content.read(chunk_size)
                if not chunk:
                    break
                f.write(chunk)

    async def retrieve_data(self, fname: str,
                            target_path: pathlib.Path) -> None:
        if target_path.exists():
            raise ClientError(f'{target_path} already exists!')
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            async with session.get(
                f'{self.server_url}/{self.SERVER_URLMAP["retrieve_data"]}/{fname}'
            ) as rsp:
                if rsp.status == 400:
                    raise ClientError(f'File {fname} not found!')
                elif rsp.status != 200:
                    raise ServerError(await rsp.text())
                else:
                    await self._save_rsp_to_file(rsp, target_path)
                    print(f'Downloaded "{fname}" to "{target_path}"')

    async def check_data(self, fname: str) -> None:
        # rsp = {status: {sha256, name, lmod, health}}
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            async with session.get(
                f'{self.server_url}/{self.SERVER_URLMAP["check_data"]}'
            ) as rsp:
                if rsp.status == 400:
                    raise ClientError(f'File {fname} not found!')
                elif rsp.status != 200:
                    raise ServerError(await rsp.text())
                else:
                    data_check = (await rsp.json())['status']
                    print('=' * 80)
                    print(f'name: {data_check["name"]}')
                    print(f'last modified: {data_check["lmod"]}')
                    print(f'health: {data_check["health"]}')
                    print(f'sha256: {data_check["sha256"]}')

    async def list_data(self) -> None:
        # rsp = {files: [{sha256, name, lmod},]}
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
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
                    print(f'sha256sum: {file_["sha256"]}')


def _run_client_fn(
        fn: typing.Callable[..., typing.Any],
        *args: typing.Any,
        **kwargs: typing.Any) -> None:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fn(*args, **kwargs))


def _setup_logging(debug: bool) -> None:
    fmt = '%(asctime)s:%(levelname)s: %(message)s'
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format=fmt)


def common_options(
    func: typing.Callable[...,
                          typing.Any]) -> typing.Callable[..., typing.Any]:
    @click.option('--debug/--no-debug',
                  default=False,
                  help='Enable debug logging')
    @click.option('-s',
                  '--server-url',
                  type=str,
                  help='Backuper Server URL',
                  default='localhost:44987')
    @click.option('-t',
                  '--timeout',
                  type=int,
                  help='Seconds before timeout',
                  default=5)
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> typing.Any:
        return func(*args, **kwargs)
    return wrapper


@click.group()
def cli() -> None:
    pass


@cli.command()
@click.argument('filename', type=str)
@click.argument('source-path', type=str)
@common_options
def submit_data(debug: bool, server_url: str, timeout: int, filename: str,
                fpath: str) -> None:
    """Submit data to archive"""
    # TODO: refactor common setup
    _setup_logging(debug)
    file_path = pathlib.Path(fpath)
    client = Client(timeout, server_url)
    _run_client_fn(client.submit_data, filename, file_path)


@cli.command()
@click.argument('filename', type=str)
@click.argument('destination-path', type=str)
@common_options
def retrieve_data(debug: bool, server_url: str, timeout: int, filename: str,
                  tpath: str) -> None:
    """Retrieve data by file name"""
    _setup_logging(debug)
    target_path = pathlib.Path(tpath)
    client = Client(timeout, server_url)
    _run_client_fn(client.retrieve_data, filename, target_path)


@cli.command()
@click.argument('filename', type=str)
@common_options
def check_data(debug: bool, server_url: str, timeout: int,
               filename: str) -> None:
    """Check data integrity"""
    _setup_logging(debug)
    client = Client(timeout, server_url)
    _run_client_fn(client.check_data, filename)


@cli.command()
@common_options
def list_data(debug: bool, server_url: str, timeout: int) -> None:
    """List data"""
    _setup_logging(debug)
    client = Client(timeout, server_url)
    _run_client_fn(client.list_data)


if __name__ == '__main__':
    cli()
