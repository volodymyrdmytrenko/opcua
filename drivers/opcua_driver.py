\
from asyncua import Client


class OpcUaDriver:
    def __init__(self, device, reconnect_delay_sec=5):
        self.device = device
        self.endpoint = device["endpoint"]
        self.client = None
        self.connected = False
        self.reconnect_delay_sec = reconnect_delay_sec

    async def connect(self):
        if self.connected:
            return
        self.client = Client(url=self.endpoint)
        await self.client.connect()
        self.connected = True

    async def disconnect(self):
        if self.client:
            try:
                await self.client.disconnect()
            except Exception:
                pass
        self.connected = False

    async def read(self, tag):
        if not self.connected:
            await self.connect()
        node = self.client.get_node(tag["nodeid"])
        return await node.read_value()
