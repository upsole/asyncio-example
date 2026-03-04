import aiofiles
import aiosqlite # No interface for prepared statements, sad!
import os
import asyncio
import os
import time
from pathlib import Path

DB_NAME = "logs.db"
SCHEMA = """
            CREATE TABLE logs (
                ts     DATETIME,
                level CHAR(6),
                body  TEXT
            );
         """
def parse_row(line : str):
    return (line[:19], line[20:26].strip(), line[26:])

async def setup_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(SCHEMA)
        await db.execute("CREATE INDEX timestamp_index ON logs ( ts )")
        await db.execute("CREATE INDEX level_index     ON logs ( level );")
        await db.commit()

async def process_log_file(db : aiosqlite.Connection, filepath : str, row_count : list[int]):
    async with aiofiles.open(filepath, "r") as f:
        contents = await f.readlines()
        params = [parse_row(line) for line in contents]
        row_count[0] += len(params)
        await db.executemany("insert into logs(ts, level, body) values (?, ?, ?)", params)

async def worker(db : aiosqlite.Connection, queue : asyncio.Queue, row_count : list[int]):
    local_counter = row_count[0]
    while True:
        filepath = await queue.get()
        await process_log_file(db, filepath, row_count)
        if ((row_count[0] - local_counter) > 100_000):
            print(f"Processed {row_count[0]:,}... rows so far")
            local_counter = row_count[0] 
        queue.task_done()

async def main():
    await setup_db()

    t0 = time.perf_counter()
    q = asyncio.Queue()
    for f in Path("target").iterdir():
        q.put_nowait(f)

    counter = [0]
    workers = []
    async with aiosqlite.connect(DB_NAME) as db:
        for _ in range(128):
            workers.append(asyncio.create_task(worker(db, q, counter)))
        await q.join()
        await db.commit()

    for w in workers:
        w.cancel()

    t_done = time.perf_counter()
    print(f"Inserted {counter[0]:,} rows in {t_done - t0 : .4f} secs into `{DB_NAME}`")
    
if __name__ == "__main__":
    try:
        os.remove(DB_NAME)
    except:
        pass

    asyncio.run(main())
