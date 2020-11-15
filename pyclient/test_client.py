from pyclient import client as pyclient

import pytest
from aioresponses import aioresponses


SERVER_URL = 'example.com:8080'
LIST_DATA_URL = f'{SERVER_URL}/{pyclient.Client.SERVER_URLMAP["list_data"]}'

list_data_parameters = [
    ({
        'files': [
            {
                'uuid': '123',
                'sha256': '456',
                'name': 'some/file/name',
                'lmod': '2020-11-14 15:49:34',
            },
        ]
    },
     '=' * 80 + '\n'
     'name: some/file/name\n'
     'last modified: 2020-11-14 15:49:34\n'
     'uuid: 123\n'
     'sha256sum: 456\n'),
    ({'files': []}, 'No files!\n'),
]


@pytest.mark.asyncio
@pytest.mark.parametrize('payload,expected', list_data_parameters)
async def test_client_list_data(capfd, payload, expected) -> None:
    with aioresponses() as m:
        m.get(LIST_DATA_URL, status=200, payload=payload)
        c = pyclient.Client(SERVER_URL)
        await c.list_data()
        captured = capfd.readouterr()
        assert captured.out == expected


@pytest.mark.asyncio
async def test_client_list_data_bad_reply(capfd) -> None:
    server_error_msg = 'Internal Server Error'
    with aioresponses() as m:
        m.get(LIST_DATA_URL, status=500, body=server_error_msg)
        c = pyclient.Client(SERVER_URL)
        with pytest.raises(pyclient.ServerError) as e:
            await c.list_data()
        assert e.value.args[0] == server_error_msg
