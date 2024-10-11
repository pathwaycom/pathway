# Copyright © 2024 Pathway


import logging
import os
import signal
import traceback
from threading import Thread

import pandas as pd
import uvicorn
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel

import pathway as pw

app = FastAPI()


class IntegratedTable:
    data: dict
    schema: pw.Schema

    def __init__(self, data, schema):
        self.data = data
        self.schema = schema

    def serialize(self):
        return pd.DataFrame.from_records(
            list(self.data.values()),
            columns=list(self.schema.keys()) + ["id"],
            index="id",
        ).to_json(default_handler=str)


registered_tables: dict[str, IntegratedTable] = {}


class RegisteredTablesResponse(BaseModel):
    tables: dict[str, list]


@app.get("/get_table")
async def get_table(alias: str):
    if alias in registered_tables:
        return Response(
            registered_tables[alias].serialize(), media_type="application/json"
        )
        return
    else:
        raise HTTPException(status_code=404, detail=f"Table `{alias}` not found")


def register_table(self: pw.Table, alias: str, *, short_pointers=True):
    integrated = {}

    def update(key, row, time, is_addition):
        row["id"] = key
        if is_addition:
            integrated[key] = tuple(row.values())
        else:
            del integrated[key]

    pw.io.subscribe(self, on_change=update)  # todo: support time end
    registered_tables[alias] = IntegratedTable(data=integrated, schema=self.schema)


def run_with_querying(*, pathway_kwargs={}, uvicorn_kwargs={}):
    def _run():
        try:
            pw.run(**pathway_kwargs)
        except Exception:
            logging.error(traceback.format_exc())
            signal.raise_signal(signal.SIGINT)

    if os.environ.get("PATHWAY_PROCESS_ID", "0") == "0":
        t = Thread(target=_run)
        t.start()
        uvicorn.run(app, loop="asyncio", **uvicorn_kwargs)
        t.join()
    else:
        _run()
