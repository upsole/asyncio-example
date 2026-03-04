import asyncio
import aiofiles
import string
import random
import os
from english_words import get_english_words_set
from datetime import datetime as dt 
from datetime import timedelta 

OUTPUT_DIR = "target"
WORDS      = list(get_english_words_set(["web2"], lower = True))
AVG_LOGSIZE = 256
MIN_FILES = 1000
MAX_FILES = 10_000
TOTAL_SIZE = 5 * 1024 * 1024 * 1024 # 5 GBs

def random_date_in(beg : dt , end : dt) -> dt:
    assert end >= beg
    d1 = end - beg
    d2 = timedelta(seconds = random.randint(0, int(d1.total_seconds())))
    return beg + d2

LOG_LEVEL = [ "TRACE", "DEBUG", "INFO", "ERROR" ]
def random_log(size : int) -> str:
    """
        size does not include the size of date + log level (25 bytes)
    """
    date = random_date_in(dt(2020, 1, 1), dt(2021, 1, 1))
    lvl  = random.choice(LOG_LEVEL)

    size_left = size
    words = []
    while size_left > 0:
        w = random.choice(WORDS)
        size_left -= len(w)
        words.append(w)
    
    body = " ".join(words)[:size - 1]
    
    return f"{date} {lvl:5} {body}"

def random_logs(file_size : int) -> str:
    n = int(file_size / AVG_LOGSIZE)

    sizes = [int(random.gauss(AVG_LOGSIZE, 128)) for x in range(n)]
    lines = [random_log(s) for s in sizes]

    return "\n".join(lines)

def find_sizes(min_files : int, max_files : int, max_total_size : int, err : float = 0.1) -> list[int]:
    """
        Searches randomly for a list of file sizes such that:
            sum(file_sizes) ~= size
    """
    ok = False
    i = 0
    threshold = int(max_total_size * err)
    while not ok:
        n = int(random.uniform(min_files, max_files))
        avg_size = max_total_size / n
        weights = [ int(random.gauss(avg_size, avg_size * 0.2)) for i in range(n) ]
        s = sum(weights)
        diff = abs ( sum(weights) - max_total_size  )
        if (diff <= threshold):
            ok = True
        i += 1

    avg = sum(weights) / len(weights)
    print(f"Founds weights in {i} attempts: {len(weights)} files (avg {avg})")
    return weights

async def writefile(filename : str, contents : str):
    path = os.path.join(OUTPUT_DIR, filename)
    async with aiofiles.open(path, mode='w') as f:
        await f.write(contents)
        print(f"Written: {filename} - {len(contents)} bytes")

async def write_worker(queue : asyncio.Queue):
    while True:
        filename, size = await queue.get()
        contents = random_logs(size)
        await writefile(filename, contents)
        queue.task_done()

async def main():
    q = asyncio.Queue()

    weights = find_sizes(MIN_FILES, MAX_FILES, TOTAL_SIZE)
    output = [(f"file{i}", size) for i, size in enumerate(weights)]
    for params in output:
        q.put_nowait(params)

    tasks = []
    for i in range(64):
        t = asyncio.create_task(write_worker(q))
        tasks.append(t)

    await q.join()

    for t in tasks:
        t.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    random.seed(50)
    os.makedirs(OUTPUT_DIR, exist_ok = True)
    asyncio.run(main())
