import asyncio
from asyncua import Client
from asyncua.common.subscription import DataChangeEvent, StatusChangeEvent

class OpcUaDriver:
    def __init__(self, device, subscription_ms=200, reconnect_delay_sec=5):
        self.device = device
        self.endpoint = device["endpoint"]
        self.subscription_ms = int(subscription_ms)
        self.reconnect_delay_sec = float(reconnect_delay_sec)
        self.client = None
        self.connected = False

    async def disconnect(self):
        if self.client is not None:
            try:
                await self.client.disconnect()
            except Exception:
                pass
        self.client = None
        self.connected = False

    async def run_subscription(self, tags, on_datachange, on_online, on_error):
        while True:
            try:
                await on_online(False)
                self.client = Client(url=self.endpoint)
                await self.client.connect(auto_reconnect=True, reconnect_max_delay=self.reconnect_delay_sec)
                self.connected = True
                await on_online(True)
                await on_error("")
                nodes = []
                tag_by_nodeid = {}
                for tag in tags:
                    node = self.client.get_node(tag["nodeid"])
                    nodes.append(node)
                    tag_by_nodeid[node.nodeid.to_string()] = tag

                async with await self.client.create_subscription(self.subscription_ms) as sub:
                    await sub.subscribe_data_change(nodes)
                    for node in nodes:
                        value = await node.read_value()
                        await on_datachange(tag_by_nodeid[node.nodeid.to_string()], value, "Good")

                    async for event in sub:
                        if isinstance(event, DataChangeEvent):
                            tag = tag_by_nodeid.get(event.node.nodeid.to_string())
                            if tag is not None:
                                await on_datachange(tag, event.value, "Good")
                        elif isinstance(event, StatusChangeEvent):
                            status = event.notification.Status
                            if status.is_bad():
                                raise ConnectionError(f"OPC UA subscription status: {status}")
            except asyncio.CancelledError:
                await self.disconnect()
                raise
            except Exception as exc:
                self.connected = False
                await on_online(False)
                await on_error(str(exc))
                print(f"[{self.device['name']}] OPC UA ERROR: {exc}")
                await self.disconnect()
                await asyncio.sleep(self.reconnect_delay_sec)
