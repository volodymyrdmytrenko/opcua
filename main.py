import asyncio, re, time
from pathlib import Path
from datetime import datetime, timezone
import yaml
from asyncua import Server, ua
from drivers.opcua_driver import OpcUaDriver
from drivers.modbus_driver import ModbusTcpDriver
from database.mysql_db import MySQLDatabase
from database.timescale_db import TimescaleWriter

BASE_DIR=Path(__file__).resolve().parent
def load_yaml(path):
    with open(BASE_DIR/path,"r",encoding="utf-8") as f: return yaml.safe_load(f)

def ua_variant_type(t):
    m={"Boolean":ua.VariantType.Boolean,"Int16":ua.VariantType.Int16,"UInt16":ua.VariantType.UInt16,
       "Int32":ua.VariantType.Int32,"UInt32":ua.VariantType.UInt32,"Float32":ua.VariantType.Float,
       "Float":ua.VariantType.Float,"Double":ua.VariantType.Double,"String":ua.VariantType.String}
    return m[t]

def initial_value(t):
    if t=="Boolean": return False
    if t in ("Int16","UInt16","Int32","UInt32"): return 0
    if t in ("Float32","Float","Double"): return 0.0
    if t=="String": return ""
    raise ValueError(t)

def sanitize(s):
    s=re.sub(r"\s+","_",str(s).strip())
    s=re.sub(r"[^A-Za-z0-9_.-]","_",s)
    return re.sub(r"_+","_",s).strip("_") or "Node"

def string_nodeid(ns,*parts): return ua.NodeId(".".join(sanitize(p) for p in parts),ns)

class HistoryPolicy:
    def __init__(self):
        self.last_value={}
        self.last_write={}
    def should_write(self, device_id, tag, value):
        h=tag.get("history",{})
        if not h.get("enabled",False): return False
        key=(device_id,tag["name"])
        mode=h.get("mode","onchange")
        if mode=="onchange":
            first=key not in self.last_value
            changed=first or self.last_value[key] != value
            self.last_value[key]=value
            return changed
        if mode=="cyclic":
            now=time.monotonic()
            interval=int(h.get("interval_ms",1000))/1000.0
            last=self.last_write.get(key)
            if last is None or now-last >= interval:
                self.last_write[key]=now
                self.last_value[key]=value
                return True
            self.last_value[key]=value
        return False

async def main():
    cfg=load_yaml("config.yaml")
    devices=load_yaml("devices.yaml")["devices"]
    rt=cfg.get("runtime",{})
    modbus_poll_ms=int(rt.get("modbus_poll_ms",200))
    sub_ms=int(rt.get("opcua_subscription_ms",200))
    reconnect=float(rt.get("reconnect_delay_sec",5))

    mysql=None
    if cfg.get("mysql",{}).get("enabled",False):
        mysql=MySQLDatabase(cfg["mysql"]); await mysql.connect(); print("MySQL connected")

    ts=None
    if cfg.get("timescale",{}).get("enabled",False):
        ts=TimescaleWriter(cfg["timescale"]); await ts.connect(); print("TimescaleDB connected")

    hist=HistoryPolicy()
    server=Server(); await server.init()
    endpoint=cfg["server"]["endpoint"]; server.set_endpoint(endpoint); server.set_server_name(cfg["server"]["name"])
    ns=await server.register_namespace(cfg["server"]["namespace"])
    root=await server.nodes.objects.add_object(string_nodeid(ns,"KONSORT"),"KONSORT")
    contexts=[]

    async def add_history(ctx, tag, value, quality="Good"):
        if ts and hist.should_write(ctx["device_id"],tag,value):
            await ts.enqueue({"time":datetime.now(timezone.utc),"device_id":ctx["device_id"],
                              "tag_name":tag["name"],"data_type":tag["type"],"value":value,"quality":quality})

    for d in devices:
        if mysql: await mysql.upsert_device(d)
        folder=await root.add_object(string_nodeid(ns,"KONSORT",d["name"]),d["name"])
        sf=await folder.add_object(string_nodeid(ns,"KONSORT",d["name"],"_Status"),"_Status")
        online=await sf.add_variable(string_nodeid(ns,"KONSORT",d["name"],"_Status","Online"),"Online",
                                     ua.Variant(False,ua.VariantType.Boolean))
        err=await sf.add_variable(string_nodeid(ns,"KONSORT",d["name"],"_Status","LastError"),"LastError",
                                  ua.Variant("",ua.VariantType.String))
        tags=load_yaml(d["tags_file"]).get("tags",[])
        targets={}
        for tag in tags:
            if mysql: await mysql.upsert_tag(d["id"],tag)
            targets[tag["name"]]=await folder.add_variable(
                string_nodeid(ns,"KONSORT",d["name"],tag["name"]),tag["name"],
                ua.Variant(initial_value(tag["type"]),ua_variant_type(tag["type"])))
        driver=OpcUaDriver(d,sub_ms,reconnect) if d["protocol"]=="opcua" else ModbusTcpDriver(d,reconnect)
        contexts.append({"device_id":d["id"],"name":d["name"],"protocol":d["protocol"],"driver":driver,
                         "tags":tags,"targets":targets,"online":online,"err":err})

    async def run_opc(ctx):
        async def on_datachange(tag,value,quality):
            await ctx["targets"][tag["name"]].write_value(ua.Variant(value,ua_variant_type(tag["type"])))
            await add_history(ctx,tag,value,quality)
        async def on_online(v): await ctx["online"].write_value(ua.Variant(bool(v),ua.VariantType.Boolean))
        async def on_error(m): await ctx["err"].write_value(ua.Variant(str(m),ua.VariantType.String))
        await ctx["driver"].run_subscription(ctx["tags"],on_datachange,on_online,on_error)

    async def run_modbus(ctx):
        while True:
            try:
                if not ctx["driver"].connected: await ctx["driver"].connect()
                await ctx["online"].write_value(ua.Variant(True,ua.VariantType.Boolean))
                await ctx["err"].write_value(ua.Variant("",ua.VariantType.String))
                for tag in ctx["tags"]:
                    v=await ctx["driver"].read(tag)
                    await ctx["targets"][tag["name"]].write_value(ua.Variant(v,ua_variant_type(tag["type"])))
                    await add_history(ctx,tag,v)
                await asyncio.sleep(modbus_poll_ms/1000)
            except asyncio.CancelledError:
                await ctx["driver"].disconnect(); raise
            except Exception as exc:
                await ctx["online"].write_value(ua.Variant(False,ua.VariantType.Boolean))
                await ctx["err"].write_value(ua.Variant(str(exc),ua.VariantType.String))
                await ctx["driver"].disconnect()
                await asyncio.sleep(reconnect)

    print(f"Starting OPC UA Gateway: {endpoint}")
    try:
        async with server:
            tasks=[asyncio.create_task(run_opc(c) if c["protocol"]=="opcua" else run_modbus(c)) for c in contexts]
            try: await asyncio.gather(*tasks)
            finally:
                for t in tasks: t.cancel()
                await asyncio.gather(*tasks,return_exceptions=True)
                for c in contexts: await c["driver"].disconnect()
    finally:
        if ts: await ts.close()
        if mysql: await mysql.close()

if __name__=="__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: print("Stopped.")
