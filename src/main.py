import random
import threading
import time
import typing

import aiohttp
from aiohttp import web
import asyncio


class WebsocketConnector:

    connections: typing.Dict[str, web.WebSocketResponse] = {}
    _connections_lock = asyncio.Lock()

    @staticmethod
    def _get_auth(request: web.Request) -> str:
        '''Check the credentials in the request and return the users id if they are ok. Raise HTTPForbidden otherwise.'''
        if request.headers.get('Authorization') in ['broker1', 'broker2']:
            return request.headers.get('Authorization')
        raise web.HTTPForbidden

    @classmethod
    async def send(cls, broker_id: str, data: dict) -> None:
        await cls.connections[broker_id].send_json(data)

    @classmethod
    async def broadcast(cls, data: dict) -> None:
        for ws in cls.connections.values():
            await ws.send_json(data)

    @classmethod
    async def broker_notifications_subscription(cls, request: web.Request) -> web.WebSocketResponse:
        broker_id = WebsocketConnector._get_auth(request)

        ws = web.WebSocketResponse()
        await ws.prepare(request)

        async with cls._connections_lock:
            cls.connections[broker_id] = ws

        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                if msg.data == "close":
                    await ws.close()
            elif msg.type == aiohttp.WSMsgType.ERROR:
                print("ws connection closed with exception %s" % ws.exception())

        async with cls._connections_lock:
            cls.connections.pop(broker_id)
        return ws

    @classmethod
    async def on_shutdown(cls, app: web.Application):
        for ws in cls.connections.values():
            await ws.close(code=1001, message='Server shutdown')


async def send_messages() -> None:
    while True:
        # We would listen for db changes here, and take appropriate
        # action when they happen. This is just here, so we can check
        # that the client gets the messages
        time.sleep(random.randint(1, 10))
        number = random.randint(1, 100)
        print('sending number', number)
        await WebsocketConnector.broadcast({'new_favorite_number': number})


def changes_listener_main() -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(send_messages())


def run_app() -> None:
    loop = asyncio.get_event_loop()
    app = web.Application(loop=loop)
    app.add_routes([web.get('/broker_notifications', WebsocketConnector.broker_notifications_subscription)])
    app.on_shutdown.append(WebsocketConnector.on_shutdown)
    web.run_app(app)


def main() -> None:
    changes_listener_thread = threading.Thread(target=changes_listener_main)
    changes_listener_thread.start()
    run_app()
    changes_listener_thread.join()


if __name__ == '__main__':
    main()
