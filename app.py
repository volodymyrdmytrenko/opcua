
import asyncio, time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import yaml
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from drivers.opcua_driver import OpcUaDriver
from drivers.modbus_driver import ModbusTcpDriver
from database.postgres import PostgresStore

BASE_DIR = Path(__file__).resolve().parent

def load_yaml(path):
    with open(BASE_DIR / path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def normalize_value(type_name, value):
    if type_name == "Boolean":
        if isinstance(value, bool): return value
        raise ValueError("Boolean value required")
    if type_name in ("Int16","Int32"):
        if isinstance(value, bool) or not isinstance(value, int): raise ValueError("Integer value required")
        return int(value)
    if type_name in ("UInt16","UInt32"):
        if isinstance(value, bool) or not isinstance(value, int): raise ValueError("Unsigned integer value required")
        if value < 0: raise ValueError("Unsigned integer cannot be negative")
        return int(value)
    if type_name in ("Float32","Float","Double"):
        if isinstance(value, bool) or not isinstance(value, (int,float)): raise ValueError("Numeric value required")
        return float(value)
    if type_name == "String":
        if not isinstance(value, str): raise ValueError("String value required")
        return value
    raise ValueError(f"Unsupported data type: {type_name}")

class WriteRequest(BaseModel):
    value: Any

class Gateway:
    def __init__(self):
        self.config = load_yaml("config.yaml")
        self.devices_cfg = load_yaml("devices.yaml").get("devices", [])
        rt = self.config.get("runtime", {})
        self.opcua_subscription_ms = int(rt.get("opcua_subscription_ms", 200))
        self.modbus_poll_ms = int(rt.get("modbus_poll_ms", 200))
        self.reconnect_delay_sec = float(rt.get("reconnect_delay_sec", 5))
        self.tags, self.devices, self.cache, self.device_status = {}, {}, {}, {}
        self.tasks = []
        self.postgres = None
        self.last_history_value, self.last_history_write = {}, {}

    async def start(self):
        pg_cfg = self.config.get("postgres", {})
        if pg_cfg.get("enabled", False):
            self.postgres = PostgresStore(pg_cfg)
            await self.postgres.connect()
            print("PostgreSQL connected")

        for device in self.devices_cfg:
            protocol = device["protocol"].lower()
            tags_cfg = load_yaml(device["tags_file"]).get("tags", [])
            if protocol == "opcua":
                driver = OpcUaDriver(device, self.opcua_subscription_ms, self.reconnect_delay_sec)
            elif protocol == "modbus_tcp":
                driver = ModbusTcpDriver(device)
            else:
                raise ValueError(f"Unsupported protocol '{protocol}' for device '{device['id']}'")

            ctx = {"config":device, "driver":driver, "tags":tags_cfg}
            self.devices[device["id"]] = ctx
            self.device_status[device["id"]] = {"online":False, "last_error":""}

            if self.postgres: await self.postgres.sync_device(device)

            for tag in tags_cfg:
                name = tag["name"]
                if name in self.tags:
                    raise ValueError(f"Duplicate REST tag name '{name}'. REST tag names must be globally unique.")
                self.tags[name] = {"config":tag, "device_id":device["id"], "driver":driver}
                self.cache[name] = {"value":None, "quality":"BadNotConnected", "timestamp":None}
                if self.postgres: await self.postgres.sync_tag(device["id"], tag)

            task = self._run_opcua_device(ctx) if protocol == "opcua" else self._run_modbus_device(ctx)
            self.tasks.append(asyncio.create_task(task))

    async def stop(self):
        for task in self.tasks: task.cancel()
        if self.tasks: await asyncio.gather(*self.tasks, return_exceptions=True)
        for ctx in self.devices.values(): await ctx["driver"].disconnect()
        if self.postgres: await self.postgres.close()

    async def _run_opcua_device(self, ctx):
        d = ctx["config"]; device_id = d["id"]
        async def on_datachange(tag, value, quality):
            await self._update_cache(device_id, tag, value, quality)
        async def on_online(value):
            self.device_status[device_id]["online"] = bool(value)
        async def on_error(message):
            self.device_status[device_id]["last_error"] = str(message)
        await ctx["driver"].run_subscription(ctx["tags"], on_datachange, on_online, on_error)

    async def _run_modbus_device(self, ctx):
        d = ctx["config"]; driver = ctx["driver"]; device_id = d["id"]
        while True:
            try:
                if not driver.connected: await driver.connect()
                self.device_status[device_id] = {"online":True, "last_error":""}
                for tag in ctx["tags"]:
                    value = await driver.read(tag)
                    await self._update_cache(device_id, tag, value, "Good")
                await asyncio.sleep(self.modbus_poll_ms / 1000)
            except asyncio.CancelledError:
                await driver.disconnect(); raise
            except Exception as exc:
                self.device_status[device_id] = {"online":False, "last_error":str(exc)}
                await driver.disconnect()
                await asyncio.sleep(self.reconnect_delay_sec)

    async def _update_cache(self, device_id, tag, value, quality):
        now = datetime.now(timezone.utc)
        self.cache[tag["name"]] = {"value":value, "quality":quality, "timestamp":now}
        if self.postgres and self._should_write_history(device_id, tag, value):
            await self.postgres.enqueue_history({
                "time":now, "tag_name":tag["name"], "device_id":device_id,
                "data_type":tag["type"], "value":value, "quality":quality,
            })

    def _should_write_history(self, device_id, tag, value):
        h = tag.get("history", {})
        if not h.get("enabled", False): return False
        key = (device_id, tag["name"]); mode = h.get("mode","onchange")
        if mode == "onchange":
            first = key not in self.last_history_value
            changed = first or self.last_history_value[key] != value
            self.last_history_value[key] = value
            return changed
        if mode == "cyclic":
            interval_ms = int(h.get("interval_ms", 1000))
            now = time.monotonic(); last = self.last_history_write.get(key)
            if last is None or (now-last)*1000 >= interval_ms:
                self.last_history_write[key] = now
                self.last_history_value[key] = value
                return True
            self.last_history_value[key] = value
        return False

    def get_tag(self, tag_name):
        ctx = self.tags.get(tag_name)
        if ctx is None: raise HTTPException(404, detail=f"Tag '{tag_name}' not found")
        device_id = ctx["device_id"]; cached = self.cache[tag_name]
        if cached["timestamp"] is None:
            raise HTTPException(503, detail={
                "message":"Tag value is not available", "tag":tag_name, "device":device_id,
                "device_online":self.device_status[device_id]["online"],
            })
        return {"tag":tag_name, "value":cached["value"], "quality":cached["quality"],
                "timestamp":cached["timestamp"], "device":device_id}

    async def write_tag(self, tag_name, raw_value):
        ctx = self.tags.get(tag_name)
        if ctx is None: raise HTTPException(404, detail=f"Tag '{tag_name}' not found")
        tag, device_id = ctx["config"], ctx["device_id"]
        if not tag.get("writable", False): raise HTTPException(403, detail=f"Tag '{tag_name}' is read-only")
        if not self.device_status[device_id]["online"]: raise HTTPException(503, detail=f"Device '{device_id}' is offline")
        try:
            value = normalize_value(tag["type"], raw_value)
        except ValueError as exc:
            raise HTTPException(422, detail=str(exc)) from exc

        try:
            await ctx["driver"].write(tag, value)
            if self.postgres: await self.postgres.log_write(tag_name, device_id, value, True)
            return {"tag":tag_name, "value":value, "status":"written", "device":device_id}
        except Exception as exc:
            if self.postgres: await self.postgres.log_write(tag_name, device_id, value, False, str(exc))
            raise HTTPException(502, detail={
                "message":"PLC write failed", "tag":tag_name, "device":device_id, "error":str(exc)
            }) from exc

gateway = Gateway()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await gateway.start()
    try:
        yield
    finally:
        await gateway.stop()

app = FastAPI(title=gateway.config["api"].get("title","KONSORT PLC REST Gateway"), lifespan=lifespan)

@app.get("/api/tags/{tag_name}")
async def get_tag(tag_name: str):
    return gateway.get_tag(tag_name)

@app.put("/api/tags/{tag_name}")
async def put_tag(tag_name: str, request: WriteRequest):
    return await gateway.write_tag(tag_name, request.value)

@app.get("/api/health")
async def health():
    return {"status":"ok", "devices":gateway.device_status}
