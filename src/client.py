'''This is an example of a consumer for the notification service. We
do not expect browsers to be direct consumers, so something like this
should be ok.'''
import aiohttp
import asyncio


async def websocket_connect():
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect('http://localhost:8080/broker_notifications', headers={'Authorization': 'broker1'}) as ws:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    if msg.data == 'close':
                        await ws.close()
                        break
                    else:
                        print('received', msg.data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    break


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(websocket_connect())
