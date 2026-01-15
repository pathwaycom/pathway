import functools

from sqlalchemy import event, func
from sqlmodel import JSON, Column, Field, Session, SQLModel, create_engine, select


class Metrics(SQLModel, table=True):
    __tablename__ = "Metrics"

    timestamp: int = Field(primary_key=True)
    worker_id: int = Field(primary_key=True)
    operator_id: int = Field(primary_key=True)
    name: str
    value: float


class MetricsAgg(SQLModel, table=True):
    __tablename__ = "MetricsAgg"

    timestamp: int = Field(primary_key=True)
    worker_id: int = Field(primary_key=True)
    operator_id: int = Field(primary_key=True)
    latency_ms: float
    rows_positive: int
    rows_negative: int


class Resources(SQLModel, table=True):
    run_id: str = Field(primary_key=True)
    graph: dict | None = Field(default_factory=dict, sa_column=Column(JSON))
    resources: dict | None = Field(default_factory=dict, sa_column=Column(JSON))

    class Config:
        arbitrary_types_allowed = True


def create_db_and_tables(
    file_path: str,
):
    url = f"sqlite:///file:{file_path}?cache=shared&uri=true"

    engine = create_engine(
        url,
        connect_args={"check_same_thread": False},
        pool_size=10,
        max_overflow=20,
        pool_recycle=1800,
        pool_pre_ping=True,
    )

    @event.listens_for(engine, "connect")
    def enable_wal_mode(dbapi_connection, _):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("PRAGMA synchronous=NORMAL;")
        cursor.close()

    SQLModel.metadata.create_all(engine)
    return engine


@functools.lru_cache
def get_graph(session: Session):
    stmt = select(Resources.graph)
    result = session.exec(stmt).one_or_none()
    return result


def get_charts_data(session: Session):
    latency_subq = (
        select(Metrics.timestamp, func.max(Metrics.value).label("max_latency"))
        .where(Metrics.name == "operator.latency")
        .group_by(Metrics.timestamp)  # type: ignore
        .subquery()
    )

    memory_subq = (
        select(Metrics.timestamp, func.max(Metrics.value).label("memory"))
        .where(Metrics.name == "process.memory.usage")
        .group_by(Metrics.timestamp)  # type: ignore
        .subquery()
    )

    latency = latency_subq.alias("latency")
    memory = memory_subq.alias("memory")

    stmt = (
        select(latency.c.timestamp, latency.c.max_latency, memory.c.memory)
        .select_from(latency)
        .join(memory, latency.c.timestamp == memory.c.timestamp)
    )
    result = session.exec(stmt).all()
    return [
        {
            "timestamp": row.timestamp,  # type: ignore
            "max_latency": row.max_latency,  # type: ignore
            "memory": row.memory,  # type: ignore
        }
        for row in result
    ]


def get_latest_data(session: Session):
    max_ts = session.exec(select(func.max(Metrics.timestamp))).one()
    stmt = select(MetricsAgg).where(
        MetricsAgg.timestamp == max_ts, MetricsAgg.operator_id.is_not(None)  # type: ignore
    )
    result = session.exec(stmt).all()
    return result


def get_available_range(session: Session):
    min_ts = session.exec(select(func.min(Metrics.timestamp))).one()
    max_ts = session.exec(select(func.max(Metrics.timestamp))).one()

    if min_ts is None or max_ts is None:
        return {"min": None, "max": None}

    min_ms = round(min_ts / 1000) * 1000
    max_ms = round(max_ts / 1000) * 1000

    return {"min": min_ms, "max": max_ms}


def get_metrics_at(timestamp: int, session: Session):
    max_ts: int | None = session.exec(
        select(func.max(Metrics.timestamp)).where(Metrics.timestamp < timestamp)
    ).one()

    if not max_ts:
        return []

    stmt = select(MetricsAgg).where(MetricsAgg.timestamp == max_ts)
    result = session.exec(stmt).all()

    return result
