import sys

from kook_client import KookClient
from kook_utils import *

from kook_signaler import *

logging.basicConfig(level=logging.DEBUG)
token = ''
channel = ''

async def task():
    while True:
        if kook_client.is_ready:
            kook_client.play()
        await asyncio.sleep(1)


async def main():
    gateway_url = get_gateway(token, channel)
    await kook_client.join(
        room_address_info={
            'server_url': gateway_url
        },
        producer_config={
            'auto_produce': True,
            'media_file_path': 'xxxx.mp3'
        }
    )


async def run():
    await asyncio.wait([
        asyncio.create_task(main())
        # asyncio.create_task(task())
    ])


if __name__ == '__main__':
    if sys.version_info.major == 3 and sys.version_info.minor == 6:
        loop = asyncio.get_event_loop()
    else:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    kook_client = KookClient()
    loop.run_until_complete(run())
