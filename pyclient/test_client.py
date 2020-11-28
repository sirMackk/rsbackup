import hashlib

from pyclient import client as pyclient

import pytest
from aioresponses import aioresponses


SERVER_URL = 'example.com:8080'
URL_MAP = pyclient.Client.SERVER_URLMAP
LIST_DATA_URL = f'http://{SERVER_URL}/{URL_MAP["list_data"]}'
CHECK_DATA_URL = f'http://{SERVER_URL}/{URL_MAP["check_data"]}'
SUBMIT_DATA_URL = f'http://{SERVER_URL}/{URL_MAP["submit_data"]}'
RETRIEVE_DATA_URL = f'http://{SERVER_URL}/{URL_MAP["retrieve_data"]}'

list_data_parameters = [
    ({
        'files': [
            {
                'name': 'some/file/name',
            },
        ]
    },
     '=' * 80 + '\n'
     'name: some/file/name\n'),
    ({'files': []}, 'No files!\n'),
]


@pytest.mark.asyncio
@pytest.mark.parametrize('payload,expected', list_data_parameters)
async def test_list_data(capfd, payload, expected) -> None:
    with aioresponses() as m:
        m.get(LIST_DATA_URL, status=200, payload=payload)
        c = pyclient.Client(server_url=SERVER_URL)
        await c.list_data()
        captured = capfd.readouterr()
        assert captured.out == expected


@pytest.mark.asyncio
async def test_list_data_bad_reply() -> None:
    server_error_msg = 'Internal Server Error'
    with aioresponses() as m:
        m.get(LIST_DATA_URL, status=500, body=server_error_msg)
        c = pyclient.Client(server_url=SERVER_URL)
        with pytest.raises(pyclient.ServerError) as e:
            await c.list_data()
        assert e.value.args[0] == server_error_msg


@pytest.mark.asyncio
async def test_check_data(capfd) -> None:
    data_name = 'some/file'
    payload = {
        'hashes': '["123", "456"]',
        'name': 'some/file',
        'lmod': '2020-11-15 15:49:34',
        'health': 'GOOD'
    }
    expected = (
        '=' * 80 + '\n'
        'name: some/file\n'
        'last modified: 2020-11-15 15:49:34\n'
        'health: GOOD\n'
        'hashes: ["123", "456"]\n'
    )
    with aioresponses() as m:
        m.get(CHECK_DATA_URL, status=200, payload=payload)
        c = pyclient.Client(server_url=SERVER_URL)
        await c.check_data(data_name)
        captured = capfd.readouterr()
        assert captured.out == expected


@pytest.mark.asyncio
@pytest.mark.parametrize('status,exc,exc_msg', [
    (400, pyclient.ClientError, 'File some/data not found!'),
    (500, pyclient.ServerError, 'Internal Server Error'),
])
async def test_check_data_bad_reply(status, exc, exc_msg) -> None:
    with aioresponses() as m:
        m.get(CHECK_DATA_URL, status=status, body=exc_msg)
        c = pyclient.Client(server_url=SERVER_URL)
        with pytest.raises(exc) as e:
            await c.check_data('some/data')
        assert e.value.args[0] == exc_msg


@pytest.mark.asyncio
async def test_submit_data(capfd, tmp_path) -> None:
    status = 200
    data_name = 'some/file'
    data_path = tmp_path / 'filetosubmit'
    data = b'1234' * 10
    with open(data_path, 'wb') as f:
        f.write(data)
    sha256_digest = hashlib.sha256(data).hexdigest()
    expected = (
        '=' * 80 + '\n' +
        f'Uploading file: {data_path}\n' +
        f'sha256: {sha256_digest}\n' +
        '=' * 80 + '\n' +
        'Status: SUCCESS\n'
        'size: 40\n' +
        'data_shards: 2\n' +
        'parity_shards: 1\n' +
        "hashes: ['123', '456']\n"
    )
    response = {
        'size': 40,
        'data_shards': 2,
        'parity_shards': 1,
        'hashes': ['123', '456'],
    }

    with aioresponses() as m:
        m.post(SUBMIT_DATA_URL, status=status, payload=response)
        c = pyclient.Client(server_url=SERVER_URL)
        await c.submit_data(data_name, data_path)
        captured = capfd.readouterr()
        assert captured.out == expected


@pytest.mark.asyncio
async def test_submit_data_no_file(capfd, tmp_path) -> None:
    data_path = tmp_path / 'not-a-file'
    expected = f'{data_path} does not exist!'

    c = pyclient.Client(server_url=SERVER_URL)
    with pytest.raises(pyclient.ClientError) as e:
        await c.submit_data('some/file', data_path)
    assert e.value.args[0] == expected


@pytest.mark.asyncio
async def test_submit_data_not_a_file(capfd, tmp_path) -> None:
    data_path = tmp_path
    expected = f'{data_path} is not a file!'

    c = pyclient.Client(server_url=SERVER_URL)
    with pytest.raises(pyclient.ClientError) as e:
        await c.submit_data('some/file', data_path)
    assert e.value.args[0] == expected


@pytest.mark.asyncio
async def test_submit_data_fails(capfd, tmp_path) -> None:
    data_path = tmp_path / 'some-file'
    data_path.touch()
    expected = 'Internal Server Error'
    with aioresponses() as m:
        m.post(SUBMIT_DATA_URL, status=500, body=expected)
        c = pyclient.Client(server_url=SERVER_URL)
        with pytest.raises(pyclient.ServerError) as e:
            await c.submit_data('some/file', data_path)
        assert e.value.args[0] == expected


@pytest.mark.asyncio
async def test_retrieve_data(capfd, tmp_path) -> None:
    data_path = tmp_path / 'target_file'
    with aioresponses() as m:
        m.get(RETRIEVE_DATA_URL + '/some/file', status=200, body=b'1234')
        c = pyclient.Client(server_url=SERVER_URL)
        await c.retrieve_data('some/file', data_path)

        with open(data_path, 'rb') as f:
            assert f.read() == b'1234'
        captured = capfd.readouterr()
        assert captured.out == f'Downloaded "some/file" to "{data_path}"\n'


@pytest.mark.asyncio
async def test_retrieve_data_already_exists(capfd, tmp_path) -> None:
    data_path = tmp_path
    c = pyclient.Client(server_url=SERVER_URL)
    with pytest.raises(pyclient.ClientError) as e:
        await c.retrieve_data('some/file', data_path)
    assert e.value.args[0] == f'{data_path} already exists!'


@pytest.mark.asyncio
@pytest.mark.parametrize('status,exc,exc_msg', [
    (400, pyclient.ServerError, 'File some/file not found!'),
    (500, pyclient.ServerError, 'Internal Server Error'),
])
async def test_retrieve_data_failure(capfd, tmp_path, status, exc,
                                     exc_msg) -> None:
    data_path = tmp_path / 'some/new/file'
    with aioresponses() as m:
        m.get(RETRIEVE_DATA_URL + '/some/file', status=status, body=exc_msg)
        c = pyclient.Client(server_url=SERVER_URL)

        with pytest.raises(exc) as e:
            await c.retrieve_data('some/file', data_path)
        assert e.value.args[0] == exc_msg
