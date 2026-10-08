import asyncio
from datetime import datetime, timezone
import asyncpg

class TimescaleWriter:
    def __init__(self, cfg):
        self.cfg = cfg
        self.pool = None
        self.queue = asyncio.Queue(maxsize=int(cfg.get("queue_maxsize",50000)))
        self.batch_size = int(cfg.get("batch_size",500))
        self.flush_interval = float(cfg.get("flush_interval_ms",250))/1000.0
        self.task = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            host=self.cfg["host"], port=int(self.cfg.get("port",5432)),
            user=self.cfg["user"], password=self.cfg["password"],
            database=self.cfg["database"], min_size=1, max_size=5)
        self.task = asyncio.create_task(self._writer_loop())

    async def close(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        if self.pool:
            await self.pool.close()

    async def enqueue(self, record):
        try:
            self.queue.put_nowait(record)
        except asyncio.QueueFull:
            print("WARNING: Timescale queue full; history record dropped")

    async def _writer_loop(self):
        while True:
            batch=[]
            try:
                batch.append(await self.queue.get())
                deadline = asyncio.get_running_loop().time()+self.flush_interval
                while len(batch) < self.batch_size:
                    remaining = deadline-asyncio.get_running_loop().time()
                    if remaining <= 0: break
                    try:
                        batch.append(await asyncio.wait_for(self.queue.get(), timeout=remaining))
                    except asyncio.TimeoutError:
                        break
                await self._flush(batch)
            except asyncio.CancelledError:
                if batch:
                    try: await self._flush(batch)
                    except Exception: pass
                raise
            except Exception as exc:
                print(f"Timescale writer error: {exc}")
                await asyncio.sleep(1)

    async def _flush(self, batch):
        rows=[]
        for r in batch:
            v=r["value"]
            rows.append((r.get("time") or datetime.now(timezone.utc),r["device_id"],r["tag_name"],r["data_type"],
                v if isinstance(v,bool) else None,
                v if isinstance(v,int) and not isinstance(v,bool) else None,
                float(v) if isinstance(v,float) else None,
                v if isinstance(v,str) else None,
                r.get("quality","Good")))
        async with self.pool.acquire() as conn:
            await conn.executemany("""INSERT INTO tag_history
                (time,device_id,tag_name,data_type,value_bool,value_int,value_double,value_text,quality)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)""", rows)
