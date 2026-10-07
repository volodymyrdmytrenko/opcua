\
import asyncio
import re
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


def sanitize_nodeid_part(value):
    """
    Keep NodeIds readable and stable.
    BrowseName may remain user-friendly, while NodeId gets a clean safe path.
    """
    value = str(value).strip()
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"[^A-Za-z0-9_.-]", "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_") or "Node"


def string_nodeid(ns_idx, *parts):
    path = ".".join(sanitize_nodeid_part(p) for p in parts)
    return ua.NodeId(path, ns_idx)


async def opcua_device_loop(ctx):
    driver = ctx["driver"]

    async def on_datachange(tag, value):
        target = ctx["target_by_name"][tag["name"]]
        await target.write_value(
            ua.Variant(value, ua_variant_type(tag["type"]))
        )

    async def on_online(value):
        await ctx["online_node"].write_value(
            ua.Variant(bool(value), ua.VariantType.Boolean)
        )

    async def on_error(message):
        await ctx["last_error_node"].write_value(
            ua.Variant(str(message), ua.VariantType.String)
        )

    await driver.run_subscription(
        tags=ctx["tags_cfg"],
        on_datachange=on_datachange,
        on_online=on_online,
        on_error=on_error,
    )


async def modbus_device_loop(ctx, poll_ms, reconnect_delay_sec):
    driver = ctx["driver"]

    while True:
        try:
            if not driver.connected:
                await driver.connect()

            await ctx["online_node"].write_value(
                ua.Variant(True, ua.VariantType.Boolean)
            )
            await ctx["last_error_node"].write_value(
                ua.Variant("", ua.VariantType.String)
            )

            for tag in ctx["tags_cfg"]:
                value = await driver.read(tag)
                target = ctx["target_by_name"][tag["name"]]
                await target.write_value(
                    ua.Variant(value, ua_variant_type(tag["type"]))
                )

            await asyncio.sleep(poll_ms / 1000.0)

        except asyncio.CancelledError:
            await driver.disconnect()
            raise

        except Exception as exc:
            await ctx["online_node"].write_value(
                ua.Variant(False, ua.VariantType.Boolean)
            )
            await ctx["last_error_node"].write_value(
                ua.Variant(str(exc), ua.VariantType.String)
            )

            print(f"[{ctx['name']}] MODBUS ERROR: {exc}")
            await driver.disconnect()
            await asyncio.sleep(reconnect_delay_sec)


async def main():
    config = load_yaml("config.yaml")
    devices_cfg = load_yaml("devices.yaml")["devices"]

    runtime = config.get("runtime", {})
    modbus_poll_ms = int(runtime.get("modbus_poll_ms", 200))
    opcua_subscription_ms = int(runtime.get("opcua_subscription_ms", 200))
    reconnect_delay_sec = float(runtime.get("reconnect_delay_sec", 5))

    server = Server()
    await server.init()

    endpoint = config["server"]["endpoint"]
    server.set_endpoint(endpoint)
    server.set_server_name(config["server"]["name"])

    namespace_uri = config["server"]["namespace"]
    ns_idx = await server.register_namespace(namespace_uri)

    # Explicit STRING NodeId: ns=<idx>;s=KONSORT
    root = await server.nodes.objects.add_object(
        string_nodeid(ns_idx, "KONSORT"),
        "KONSORT",
    )

    contexts = []

    for device in devices_cfg:
        name = device["name"]
        protocol = device["protocol"].lower()

        device_folder = await root.add_object(
            string_nodeid(ns_idx, "KONSORT", name),
            name,
        )

        status_folder = await device_folder.add_object(
            string_nodeid(ns_idx, "KONSORT", name, "_Status"),
            "_Status",
        )

        online_node = await status_folder.add_variable(
            string_nodeid(ns_idx, "KONSORT", name, "_Status", "Online"),
            "Online",
            ua.Variant(False, ua.VariantType.Boolean),
        )

        last_error_node = await status_folder.add_variable(
            string_nodeid(ns_idx, "KONSORT", name, "_Status", "LastError"),
            "LastError",
            ua.Variant("", ua.VariantType.String),
        )

        tags_cfg = load_yaml(device["tags_file"]).get("tags", [])
        target_by_name = {}

        seen_names = set()
        for tag in tags_cfg:
            tag_name = tag["name"]

            if tag_name in seen_names:
                raise ValueError(
                    f"Duplicate tag name '{tag_name}' in {device['tags_file']}"
                )
            seen_names.add(tag_name)

            target_by_name[tag_name] = await device_folder.add_variable(
                string_nodeid(ns_idx, "KONSORT", name, tag_name),
                tag_name,
                ua.Variant(
                    initial_value(tag["type"]),
                    ua_variant_type(tag["type"]),
                ),
            )

        if protocol == "opcua":
            driver = OpcUaDriver(
                device,
                subscription_ms=opcua_subscription_ms,
                reconnect_delay_sec=reconnect_delay_sec,
            )
        elif protocol == "modbus_tcp":
            driver = ModbusTcpDriver(
                device,
                reconnect_delay_sec=reconnect_delay_sec,
            )
        else:
            raise ValueError(f"Unsupported protocol: {protocol}")

        contexts.append(
            {
                "name": name,
                "protocol": protocol,
                "driver": driver,
                "tags_cfg": tags_cfg,
                "target_by_name": target_by_name,
                "online_node": online_node,
                "last_error_node": last_error_node,
            }
        )

    print(f"Starting OPC UA Gateway: {endpoint}")
    print(f"Namespace index: {ns_idx}")
    print("Output NodeIds: explicit String NodeIds")
    print(
        f"OPC UA source mode: subscriptions "
        f"({opcua_subscription_ms} ms publishing interval)"
    )
    print(f"Modbus source mode: polling ({modbus_poll_ms} ms)")

    async with server:
        tasks = []

        for ctx in contexts:
            if ctx["protocol"] == "opcua":
                tasks.append(asyncio.create_task(opcua_device_loop(ctx)))
            elif ctx["protocol"] == "modbus_tcp":
                tasks.append(
                    asyncio.create_task(
                        modbus_device_loop(
                            ctx,
                            modbus_poll_ms,
                            reconnect_delay_sec,
                        )
                    )
                )

        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()

            await asyncio.gather(*tasks, return_exceptions=True)

            for ctx in contexts:
                await ctx["driver"].disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped.")
