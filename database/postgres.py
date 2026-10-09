
import asyncio
from datetime import datetime, timezone
import asyncpg

class PostgresStore:
    def __init__(self, cfg):
        self.cfg = cfg
        self.pool = None
        h = cfg.get("history", {})
        self.queue = asyncio.Queue(maxsize=int(h.get("queue_maxsize", 50000)))
        self.batch_size = int(h.get("batch_size", 500))
        self.flush_interval = float(h.get("flush_interval_ms", 250)) / 1000.0
        self.writer_task = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            host=self.cfg["host"], port=int(self.cfg.get("port", 5432)),
            user=self.cfg["user"], password=self.cfg["password"],
            database=self.cfg["database"], min_size=1, max_size=5,
        )
        self.writer_task = asyncio.create_task(self._history_writer())

    async def close(self):
        if self.writer_task:
            self.writer_task.cancel()
            await asyncio.gather(self.writer_task, return_exceptions=True)
        if self.pool:
            await self.pool.close()

    async def sync_device(self, d):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO devices(device_id,name,protocol,endpoint,host,port,unit_id,enabled)
                VALUES($1,$2,$3,$4,$5,$6,$7,TRUE)
                ON CONFLICT(device_id) DO UPDATE SET
                  name=EXCLUDED.name, protocol=EXCLUDED.protocol, endpoint=EXCLUDED.endpoint,
                  host=EXCLUDED.host, port=EXCLUDED.port, unit_id=EXCLUDED.unit_id, enabled=TRUE
            """, d["id"], d["name"], d["protocol"], d.get("endpoint"),
               d.get("host"), d.get("port"), d.get("unit_id"))

    async def sync_tag(self, device_id, tag):
        hist = tag.get("history", {})
        src = tag.get("nodeid") or f"{tag.get('area')}:{tag.get('address')}"
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO tags(tag_name,device_id,data_type,source_address,writable,
                  history_enabled,history_mode,history_interval_ms,enabled)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,TRUE)
                ON CONFLICT(tag_name) DO UPDATE SET
                  device_id=EXCLUDED.device_id, data_type=EXCLUDED.data_type,
                  source_address=EXCLUDED.source_address, writable=EXCLUDED.writable,
                  history_enabled=EXCLUDED.history_enabled, history_mode=EXCLUDED.history_mode,
                  history_interval_ms=EXCLUDED.history_interval_ms, enabled=TRUE
            """, tag["name"], device_id, tag["type"], src, bool(tag.get("writable", False)),
               bool(hist.get("enabled", False)), hist.get("mode","disabled"), hist.get("interval_ms"))

    async def log_write(self, tag_name, device_id, requested_value, success, error_message=None):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO write_log(time,tag_name,device_id,requested_value,success,error_message)
                VALUES($1,$2,$3,$4,$5,$6)
            """, datetime.now(timezone.utc), tag_name, device_id, str(requested_value),
               bool(success), error_message)

    async def enqueue_history(self, record):
        try:
            self.queue.put_nowait(record)
        except asyncio.QueueFull:
            print("WARNING: PostgreSQL history queue full; record dropped")

    async def _history_writer(self):
        while True:
            batch = []
            try:
                batch.append(await self.queue.get())
                deadline = asyncio.get_running_loop().time() + self.flush_interval
                while len(batch) < self.batch_size:
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0: break
                    try:
                        batch.append(await asyncio.wait_for(self.queue.get(), timeout=remaining))
                    except asyncio.TimeoutError:
                        break
                await self._flush_history(batch)
            except asyncio.CancelledError:
                if batch:
                    try: await self._flush_history(batch)
                    except Exception: pass
                raise
            except Exception as exc:
                print(f"PostgreSQL history writer error: {exc}")
                await asyncio.sleep(1)

    async def _flush_history(self, batch):
        rows = []
        for r in batch:
            v = r["value"]
            rows.append((
                r["time"], r["tag_name"], r["device_id"], r["data_type"],
                v if isinstance(v,bool) else None,
                v if isinstance(v,int) and not isinstance(v,bool) else None,
                float(v) if isinstance(v,float) else None,
                v if isinstance(v,str) else None,
                r.get("quality","Good"),
            ))
        async with self.pool.acquire() as conn:
            await conn.executemany("""
                INSERT INTO tag_history(time,tag_name,device_id,data_type,
                  value_bool,value_int,value_double,value_text,quality)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
            """, rows)
