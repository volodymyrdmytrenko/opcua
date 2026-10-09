
import asyncio
from asyncua import Client, ua
from asyncua.common.subscription import DataChangeEvent, StatusChangeEvent

TYPE_MAP = {
    "Boolean": ua.VariantType.Boolean,
    "Int16": ua.VariantType.Int16,
    "UInt16": ua.VariantType.UInt16,
    "Int32": ua.VariantType.Int32,
    "UInt32": ua.VariantType.UInt32,
    "Float32": ua.VariantType.Float,
    "Float": ua.VariantType.Float,
    "Double": ua.VariantType.Double,
    "String": ua.VariantType.String,
}

class OpcUaDriver:
    def __init__(self, device, subscription_ms=200, reconnect_delay_sec=5):
        self.device = device
        self.endpoint = device["endpoint"]
        self.subscription_ms = int(subscription_ms)
        self.reconnect_delay_sec = float(reconnect_delay_sec)
        self.client = None
        self.connected = False

    async def connect(self):
        if self.connected and self.client is not None:
            return
        self.client = Client(url=self.endpoint)
        await self.client.connect(
            auto_reconnect=True,
            reconnect_max_delay=self.reconnect_delay_sec,
        )
        self.connected = True

    async def disconnect(self):
        if self.client is not None:
            try:
                await self.client.disconnect()
            except Exception:
                pass
        self.client = None
        self.connected = False

    async def write(self, tag, value):
        if not self.connected or self.client is None:
            raise ConnectionError("OPC UA device is offline")
        node = self.client.get_node(tag["nodeid"])
        await node.write_value(ua.Variant(value, TYPE_MAP[tag["type"]]))

    async def run_subscription(self, tags, on_datachange, on_online, on_error):
        while True:
            try:
                await on_online(False)
                await self.connect()
                await on_online(True)
                await on_error("")

                nodes = []
                tag_by_nodeid = {}
                for tag in tags:
                    node = self.client.get_node(tag["nodeid"])
                    nodes.append(node)
                    tag_by_nodeid[node.nodeid.to_string()] = tag

                async with await self.client.create_subscription(self.subscription_ms) as subscription:
                    await subscription.subscribe_data_change(nodes)

                    for node in nodes:
                        value = await node.read_value()
                        tag = tag_by_nodeid[node.nodeid.to_string()]
                        await on_datachange(tag, value, "Good")

                    async for event in subscription:
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
