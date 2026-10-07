\
import struct
from pymodbus.client import AsyncModbusTcpClient


class ModbusTcpDriver:
    def __init__(self, device, reconnect_delay_sec=5):
        self.device = device
        self.host = device["host"]
        self.port = int(device.get("port", 502))
        self.unit_id = int(device.get("unit_id", 1))
        self.client = None
        self.connected = False
        self.reconnect_delay_sec = reconnect_delay_sec

    async def connect(self):
        if self.connected and self.client:
            return

        self.client = AsyncModbusTcpClient(self.host, port=self.port)
        ok = await self.client.connect()
        self.connected = bool(ok)

        if not self.connected:
            raise ConnectionError(
                f"Cannot connect to Modbus TCP device {self.host}:{self.port}"
            )

    async def disconnect(self):
        if self.client:
            try:
                self.client.close()
            except Exception:
                pass

        self.client = None
        self.connected = False

    async def read(self, tag):
        if not self.connected:
            await self.connect()

        area = tag["area"]
        address = int(tag["address"])
        dtype = tag["type"]

        if area == "coil":
            rr = await self.client.read_coils(
                address=address, count=1, device_id=self.unit_id
            )
            if rr.isError():
                raise IOError(str(rr))
            return bool(rr.bits[0])

        if area == "discrete_input":
            rr = await self.client.read_discrete_inputs(
                address=address, count=1, device_id=self.unit_id
            )
            if rr.isError():
                raise IOError(str(rr))
            return bool(rr.bits[0])

        count = 2 if dtype in ("Float32", "Int32", "UInt32") else 1

        if area == "holding_register":
            rr = await self.client.read_holding_registers(
                address=address, count=count, device_id=self.unit_id
            )
        elif area == "input_register":
            rr = await self.client.read_input_registers(
                address=address, count=count, device_id=self.unit_id
            )
        else:
            raise ValueError(f"Unsupported Modbus area: {area}")

        if rr.isError():
            raise IOError(str(rr))

        regs = rr.registers

        if dtype == "UInt16":
            return int(regs[0])

        if dtype == "Int16":
            value = regs[0]
            return value - 65536 if value > 32767 else value

        if dtype in ("UInt32", "Int32", "Float32"):
            if tag.get("word_order", "big") == "little":
                regs = list(reversed(regs))

            raw = struct.pack(">HH", regs[0], regs[1])

            if dtype == "UInt32":
                return struct.unpack(">I", raw)[0]
            if dtype == "Int32":
                return struct.unpack(">i", raw)[0]
            if dtype == "Float32":
                return struct.unpack(">f", raw)[0]

        raise ValueError(f"Unsupported Modbus type: {dtype}")
