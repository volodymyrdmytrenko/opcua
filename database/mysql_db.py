import asyncmy

class MySQLDatabase:
    def __init__(self, cfg):
        self.cfg = cfg
        self.pool = None

    async def connect(self):
        self.pool = await asyncmy.create_pool(
            host=self.cfg["host"], port=int(self.cfg.get("port",3306)),
            user=self.cfg["user"], password=self.cfg["password"],
            db=self.cfg["database"], minsize=1, maxsize=5, autocommit=True)

    async def close(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

    async def upsert_device(self, device):
        sql = """INSERT INTO devices(device_id,name,protocol,endpoint,host,port,unit_id,enabled)
                 VALUES(%s,%s,%s,%s,%s,%s,%s,TRUE)
                 ON DUPLICATE KEY UPDATE name=VALUES(name),protocol=VALUES(protocol),
                 endpoint=VALUES(endpoint),host=VALUES(host),port=VALUES(port),unit_id=VALUES(unit_id),enabled=TRUE"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql,(device["id"],device["name"],device["protocol"],
                    device.get("endpoint"),device.get("host"),device.get("port"),device.get("unit_id")))

    async def upsert_tag(self, device_id, tag):
        hist = tag.get("history",{})
        src = tag.get("nodeid") or f"{tag.get('area')}:{tag.get('address')}"
        sql = """INSERT INTO tags(device_id,tag_name,data_type,source_address,history_enabled,history_mode,history_interval_ms,enabled)
                 VALUES(%s,%s,%s,%s,%s,%s,%s,TRUE)
                 ON DUPLICATE KEY UPDATE data_type=VALUES(data_type),source_address=VALUES(source_address),
                 history_enabled=VALUES(history_enabled),history_mode=VALUES(history_mode),
                 history_interval_ms=VALUES(history_interval_ms),enabled=TRUE"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql,(device_id,tag["name"],tag["type"],src,
                    bool(hist.get("enabled",False)),hist.get("mode","disabled"),hist.get("interval_ms")))
