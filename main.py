import pandas as pd
from sqlalchemy import create_engine
from openpyxl import Workbook
import aiohttp
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()


HOST = os.environ.get("host", "127.0.0.1")
PORT = os.environ.get("port", "5432")
USER = os.environ.get("user", "postgres")
PASSWORD = os.environ.get("password", "postgres")
DB = os.environ.get("db", "yarmiac")
API = os.environ.get("api", "http://127.0.0.1:15426/api/getCusFioById?cusId={cus_id}")

DB_DSN = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"


CHUNK_SIZE_DB = 10_000
SUB_BATCH_SIZE = 1000
ASYNC_LIMIT = 30
REQUEST_TIMEOUT = 10

OUTPUT_FILE = "result.xlsx"
ROW_LIMIT = 1_048_575


async def fetch_fio(session, semaphore, cus_id):
    async with semaphore:
        try:
            async with session.get(
                API.format(cus_id=cus_id),
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            ) as resp:
                if resp.status != 200:
                    return None

                data = await resp.json()
                return data.get("fio")

        except Exception:
            return None


async def fetch_fios(session, semaphore, cus_ids):
    tasks = [
        fetch_fio(session, semaphore, cid)
        for cid in cus_ids
    ]
    return await asyncio.gather(*tasks)


async def async_main():
    engine = create_engine(DB_DSN)

    wb = Workbook(write_only=True)
    ws = wb.create_sheet()
    ws.append(["FIO"])

    semaphore = asyncio.Semaphore(ASYNC_LIMIT)

    total = 0

    query = f"SELECT cus_id FROM transactions LIMIT {ROW_LIMIT}"

    async with aiohttp.ClientSession() as session:
        for chunk in pd.read_sql(query, engine, chunksize=CHUNK_SIZE_DB):
            cus_ids = chunk["cus_id"].tolist()

            for i in range(0, len(cus_ids), SUB_BATCH_SIZE):
                sub_ids = cus_ids[i:i + SUB_BATCH_SIZE]

                fios = await fetch_fios(session, semaphore, sub_ids)

                for fio in fios:
                    if fio:
                        ws.append([fio])

                total += len(sub_ids)
                print(f"processed: {total}")

    wb.save(OUTPUT_FILE)

asyncio.run(async_main())
