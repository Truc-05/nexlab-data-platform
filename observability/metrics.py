import time
import json
import os
from contextlib import contextmanager
from typing import Any
from observability.logger import get_logger

logger = get_logger("metrics")

_metrics_store: list[dict] = []

def record_metric(name: str, value: float, **tags: Any):
    entry = {
        "metric": name,
        "value": value,
        "ts": time.time(),
        **tags,
    }
    _metrics_store.append(entry)
    logger.info("metric_recorded", metric=name, value=value, **tags)

@contextmanager
def timer(job_name: str, **tags: Any):
    start = time.time()
    try:
        yield
    finally:
        duration = round(time.time() - start, 3)
        record_metric("job_duration_seconds", duration, job=job_name, **tags)

def get_data_freshness_lag(last_partition_ts: float) -> float:
    lag_seconds = time.time() - last_partition_ts
    record_metric("data_freshness_lag_seconds", lag_seconds)
    return lag_seconds

def dump_metrics(path: str = None):
    if path is None:
        path = os.path.join(os.getcwd(), "metrics_dump.json")
    with open(path, "w") as f:
        json.dump(_metrics_store, f, indent=2)
    logger.info("metrics_dumped", path=path, count=len(_metrics_store))

def get_all_metrics() -> list[dict]:
    return list(_metrics_store)
