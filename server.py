import asyncio
import json
import sys
import os
from aiomqtt import Client, MqttError
from loguru import logger
from typing import Optional


class ServerBridge(asyncio.Protocol):
    def __init__(self, mqtt: Client, sn: str):
        self._mqtt = mqtt
        self._mqtt_queue = asyncio.PriorityQueue()
        self._down_topic_base = f'gw/{sn}/tcp/down/'
        self._client: Optional[asyncio.Transport] = None

        asyncio.create_task(self._mqtt_tx_loop())
        asyncio.create_task(self._start_server())


    async def _mqtt_tx_loop(self):
        open = False
        while True:
            qos, data = await self._mqtt_queue.get()
            logger.trace(f'item qos={qos}')
            if qos == 0:
                logger.trace(f'cmd={data}')
                if data == 'open':
                    msg = {'host': '127.0.0.1', 'port': 9022}
                    # msg = {'host': '192.168.3.90', 'port': 1022}
                    s = json.dumps(msg)
                    open = await self._mqtt_pubish('open', s.encode())
                    if not open:
                        self.socket_close()
                elif data == 'lost':
                    await self._mqtt_pubish('lost', b'0', timeout=10)
                    open = False
            elif qos == 1:
                if open:
                    await self._mqtt_pubish('data', data)
            else:
                logger.error(f'invalid qos={qos}, data={data}')


    async def _mqtt_pubish(self, name: str, data: bytes, timeout: float = 1.0):
        try:
            await self._mqtt.publish(topic=f'{self._down_topic_base}{name}', payload=data, qos=1, timeout=timeout)
            return True
        except MqttError:
            logger.warning(f'publish {name} timeout')
            return False
        

    def socket_send(self, data: bytes):
        if self._client:
            logger.debug(f'send {len(data)}')
            self._client.write(data)


    def socket_close(self):
        if self._client:
            logger.debug('close')
            self._client.close()
            self._client = None


    async def _start_server(self):
        loop = asyncio.get_running_loop()
        server: asyncio.AbstractServer = await loop.create_server(
            protocol_factory=lambda: self, host='127.0.0.1', port=12345, backlog=1)
        logger.info('server open')
        async with server:
            await server.wait_closed()
        logger.info('server closed')

    
    def connection_made(self, transport: asyncio.Transport):
        if self._client:
            logger.warning('another client')
            transport.close()

        logger.info('new client')
        self._client = transport

        async def _new_connect():
            await self._mqtt_queue.put((0, 'open'))
        asyncio.create_task(_new_connect())


    def connection_lost(self, exc: Optional[Exception]):
        logger.info(f'client lost')
        if exc:
            logger.info(exc)
        
        async def _new_lost():
            await self._mqtt_queue.put((0, 'lost'))
        asyncio.create_task(_new_lost())

        self._client = None


    def data_received(self, data: bytes):
        logger.trace(f'recv {len(data)}')
        if len(data) == 0:
            return
        
        async def _new_data():
            await self._mqtt_queue.put((1, data))
        asyncio.create_task(_new_data())


    def eof_received(self):
        return True


async def main():
    env = os.environ
    gateway_sn = env['GATEWAY_SN']
    assert len(gateway_sn) == 12, f'invalid $GATEWAY_SN={gateway_sn}'
    mqtt_host = env['MQTT_HOST']
    assert len(mqtt_host) > 0, f'invalid $MQTT_HOST={mqtt_host}'

    async with Client(hostname=mqtt_host, username='admin', password='admin',
        identifier='tcp_server', timeout=10, clean_session=True) as c:
        up_topic_base = f'gw/{gateway_sn}/tcp/up/'
        await c.subscribe(f'{up_topic_base}+', timeout=3)

        server = ServerBridge(c, gateway_sn)

        async for message in c.messages:
            topic = message.topic.value
            logger.trace(f'up topic={topic}, len={len(message.payload)}')

            if len(topic) <= len(up_topic_base):
                logger.error('topic too short')
                continue

            cmd = topic[len(up_topic_base):]
            if cmd == 'data':
                server.socket_send(message.payload)
            elif cmd == 'end':
                server.socket_close()
            else:
                logger.error(f'invalid cmd={cmd}')


def logger_init():
    logger.remove()
    logger.add(sys.stdout, level='TRACE')
    logger.add('logs/server.log', rotation='8MB', retention='7 days', compression='tar.xz')


if __name__ == '__main__':
    logger_init()
    asyncio.run(main())
