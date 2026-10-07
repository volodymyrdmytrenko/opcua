\
import asyncio
from pathlib import Path
import yaml

from asyncua import Server, ua

from drivers.opcua_driver import OpcUaDriver
from drivers.modbus_driver import ModbusTcpDriver


BASE_DIR = Path(__file__).resolve().parent


def load_yaml(path):
    with open(BASE_DIR / path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ua_variant_type(type_name):
    mapping = {
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
    if type_name not in mapping:
        raise ValueError(f"Unsupported OPC UA target type: {type_name}")
    return mapping[type_name]


def initial_value(type_name):
    if type_name == "Boolean":
        return False
    if type_name in ("Int16", "UInt16", "Int32", "UInt32"):
        return 0
    if type_name in ("Float32", "Float", "Double"):
        return 0.0
    if type_name == "String":
        return ""
    raise ValueError(f"Unsupported type: {type_name}")


async def device_loop(device_ctx, poll_ms, reconnect_delay_sec):
    driver = device_ctx["driver"]
    tags = device_ctx["tags"]
    online_node = device_ctx["online_node"]
    last_error_node = device_ctx["last_error_node"]

    while True:
        try:
            if not driver.connected:
                await driver.connect()

            await online_node.write_value(True)
            await last_error_node.write_value("")

            for tag_ctx in tags:
                value = await driver.read(tag_ctx["config"])
                variant_type = ua_variant_type(tag_ctx["config"]["type"])
                await tag_ctx["node"].write_value(ua.Variant(value, variant_type))

            await asyncio.sleep(poll_ms / 1000.0)

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            await online_node.write_value(False)
            await last_error_node.write_value(str(exc))
            print(f"[{device_ctx['name']}] ERROR: {exc}")
            await driver.disconnect()
            await asyncio.sleep(reconnect_delay_sec)


async def main():
    config = load_yaml("config.yaml")
    devices_cfg = load_yaml("devices.yaml")["devices"]

    server = Server()
    await server.init()

    endpoint = config["server"]["endpoint"]
    server.set_endpoint(endpoint)
    server.set_server_name(config["server"]["name"])

    namespace_uri = config["server"]["namespace"]
    ns_idx = await server.register_namespace(namespace_uri)

    objects = server.nodes.objects
    root = await objects.add_object(ns_idx, "KONSORT")

    runtime = config.get("runtime", {})
    poll_ms = int(runtime.get("poll_ms", 200))
    reconnect_delay_sec = int(runtime.get("reconnect_delay_sec", 5))

    device_contexts = []

    for device in devices_cfg:
        device_folder = await root.add_object(ns_idx, device["name"])
        status_folder = await device_folder.add_object(ns_idx, "_Status")

        online_node = await status_folder.add_variable(
            ns_idx, "Online", ua.Variant(False, ua.VariantType.Boolean)
        )
        last_error_node = await status_folder.add_variable(
            ns_idx, "LastError", ua.Variant("", ua.VariantType.String)
        )

        tags_cfg = load_yaml(device["tags_file"]).get("tags", [])
        tag_contexts = []

        for tag in tags_cfg:
            node = await device_folder.add_variable(
                ns_idx,
                tag["name"],
                ua.Variant(initial_value(tag["type"]), ua_variant_type(tag["type"])),
            )
            tag_contexts.append({"config": tag, "node": node})

        protocol = device["protocol"].lower()

        if protocol == "opcua":
            driver = OpcUaDriver(device, reconnect_delay_sec)
        elif protocol == "modbus_tcp":
            driver = ModbusTcpDriver(device, reconnect_delay_sec)
        else:
            raise ValueError(f"Unsupported protocol: {protocol}")

        device_contexts.append(
            {
                "name": device["name"],
                "driver": driver,
                "tags": tag_contexts,
                "online_node": online_node,
                "last_error_node": last_error_node,
            }
        )

    print(f"Starting OPC UA Gateway: {endpoint}")

    async with server:
        tasks = [
            asyncio.create_task(device_loop(ctx, poll_ms, reconnect_delay_sec))
            for ctx in device_contexts
        ]

        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            for ctx in device_contexts:
                await ctx["driver"].disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped.")
