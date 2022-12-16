from dataclasses import dataclass
import logging
from urllib.parse import urlparse

import requests


def get_gateway(token: str, channel_id: str, bot: bool = True) -> str:
    header = {
        'Authorization': f'Bot {token}' if bot else token
    }
    r = requests.get(f'https://www.kaiheila.cn/api/v3/gateway/voice?channel_id={channel_id}', headers=header)
    data = r.json()
    if data['code'] != 0:
        raise RuntimeError(data['message'])
    gateway_url = data['data']['gateway_url']
    logging.debug(f'KOOK Voice Gateway url: {gateway_url}')
    return gateway_url


def url_parse(url: str):
    url_query = {}
    url_parsed = urlparse(url)
    for i in url_parsed.query.split('&'):
        query = str(i)
        index = query.find('=')
        url_query[query[:index]] = query[index + 1:]
    return url_parsed.hostname, url_parsed.port, url_parsed.path, url_query


def gateway_url_parse(url: str):
    url_query = url_parse(url)[3]
    return GatewayQuery(url_query['token'], url_query['roomId'], url_query['peerId'], url_query['sign'])


@dataclass
class GatewayQuery:
    token: str
    room_id: str
    peer_id: str
    sign: str
