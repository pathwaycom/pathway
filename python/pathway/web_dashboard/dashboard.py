import glob
import logging
import os
from pathlib import Path
from typing import AsyncIterator

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from sqlmodel import Session

from . import db

current_dir = Path(__file__).resolve().parent


async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    metrics_directory = os.environ.get("PATHWAY_DETAILED_METRICS_DIR", ".")
    pattern = os.path.join(metrics_directory, "metrics_*.db")
    files = glob.glob(pattern)

    if not files:
        raise FileNotFoundError("No metrics database found")

    latest = max(files, key=os.path.getmtime)

    logging.info(f"Reading metrics from: {latest}")

    app.state.db_engine = db.create_db_and_tables(latest)
    try:
        yield
    finally:
        app.state.db_engine.dispose()


async def get_session(request: Request):
    engine = request.app.state.db_engine
    session = Session(engine)
    try:
        yield session
    finally:
        session.close()


app = FastAPI(lifespan=lifespan)  # type: ignore

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # List of allowed origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)


@app.get("/metrics/latest")
def get_metrics_latest(session: Session = Depends(get_session)):
    return db.get_latest_data(session)


@app.get("/metrics/available_range")
def get_metrics_range(session: Session = Depends(get_session)):
    return db.get_available_range(session)


@app.get("/metrics/at/{timestamp}")
def get_metrics_at(timestamp: int, session: Session = Depends(get_session)):
    return db.get_metrics_at(timestamp, session)


@app.get("/graph")
def get_graph(session: Session = Depends(get_session)):
    return db.get_graph(session)


@app.get("/metrics/charts")
def get_charts_data(session: Session = Depends(get_session)):
    return db.get_charts_data(session)


app.mount(
    "/",
    StaticFiles(directory=current_dir / "frontend", html=True),
    name="frontend",
)
