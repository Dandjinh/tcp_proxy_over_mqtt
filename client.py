import asyncio
import json
import sys
import os
from aiomqtt import Client, MqttError
from loguru import logger
from typing import Optional


class ClientBridge:
    _CLOSE_WAIT = 60

    def __init__(self, mqtt: Client, sn: str):
        self._mqtt = mqtt
        self._mqtt_queue = asyncio.PriorityQueue()
        self._up_topic_base = f'gw/{sn}/tcp/up/'
        self._writer: Optional[asyncio.StreamWriter] = None
        self._close_time = 0
        self._close_task: Optional[asyncio.Task] = None


    def update_close_time(self):
        loop = asyncio.get_running_loop()
        self._close_time = loop.time() + ClientBridge._CLOSE_WAIT


    async def _mqtt_pubish(self, name: str, data: bytes, timeout: float = 1.0):
        try:
            await self._mqtt.publish(topic=f'{self._up_topic_base}{name}', payload=data, qos=1, timeout=timeout)
            return True
        except MqttError:
            logger.warning(f'publish {name} timeout')
            return False
    

    async def tcp_connect(self, host: str, port: int):
        logger.info(f'try open {host}:{port}')

        if self._writer:
            logger.warning('another open')
            await self.tcp_close()
            return

        try:
            conn = asyncio.open_connection(host=host, port=port)
            reader, writer = await asyncio.wait_for(conn, 3.0)
            logger.info('open ok')
            self._writer = writer
            asyncio.create_task(self._tcp_loop(reader))
            self.update_close_time()
            self._close_task = asyncio.create_task(self.close_loop())
        except:
            logger.error(f'open {host}:{port} fail')
            await self._mqtt_pubish('end', b'0', timeout=10)


    async def tcp_close(self):
        self._close_time = 0
        if self._close_task:
            self._close_task.cancel()

        if self._writer:
            logger.debug('close')
            self._writer.close()
            self._writer = None
            await self._mqtt_pubish('end', b'0', timeout=10)


    def tcp_send(self, data: bytes):
        if self._writer:
            logger.debug(f'send {len(data)}')
            self._writer.write(data)
            self.update_close_time()


    async def _tcp_loop(self, reader: asyncio.StreamReader):
        while self._writer and not reader.at_eof():
            try:
                rx = await reader.read(4096)
                logger.trace(f'recv {len(rx)}')
                if len(rx) == 0:
                    continue

                await self._mqtt_pubish('data', rx)
                self.update_close_time()
            except ConnectionResetError:
                logger.warning('reset by peer')
                break

        logger.info('loop end')
        await self.tcp_close()


    async def close_loop(self):
        loop = asyncio.get_running_loop()
        while True:
            now = loop.time()
            if now >= self._close_time:
                logger.warning('close timeout')
                self._close_task = None
                await self.tcp_close()
                break

            await asyncio.sleep(2)


async def main():
    env = os.environ
    gateway_sn = env['GATEWAY_SN']
    assert len(gateway_sn) == 12, f'invalid $GATEWAY_SN={gateway_sn}'
    mqtt_host = env['MQTT_HOST']
    assert len(mqtt_host) > 0, f'invalid $MQTT_HOST={mqtt_host}'

    async with Client(hostname=mqtt_host, username='admin', password='admin',
        identifier='tcp_client', timeout=10, clean_session=True) as c:
        down_topic_base = f'gw/{gateway_sn}/tcp/down/'
        await c.subscribe(f'{down_topic_base}+', timeout=3)

        client = ClientBridge(c, gateway_sn)

        async for message in c.messages:
            topic = message.topic.value
            logger.trace(f'down topic={topic}, len={len(message.payload)}')

            if len(topic) <= len(down_topic_base):
                logger.error('topic too short')
                continue

            cmd = topic[len(down_topic_base):]
            if cmd == 'data':
                client.tcp_send(message.payload)
            elif cmd == 'open':
                info = json.loads(message.payload.decode())
                await client.tcp_connect(info['host'], info['port'])
            elif cmd == 'lost':
                await client.tcp_close()
            else:
                logger.error(f'invalid cmd={cmd}')


def logger_init():
    logger.remove()
    logger.add(sys.stdout, level='TRACE')
    logger.add('logs/client.log', rotation='8MB', retention='7 days', compression='tar.xz')


if __name__ == '__main__':
    logger_init()
    asyncio.run(main())
