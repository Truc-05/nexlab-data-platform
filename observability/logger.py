import logging
import json
from typing import Any

class StructuredFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if hasattr(record, "extra"):
            payload.update(record.extra)
        return json.dumps(payload)

def get_logger(name: str) -> "StructuredLogger":
    return StructuredLogger(name)

class StructuredLogger:
    def __init__(self, name: str):
        self._logger = logging.getLogger(name)
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(StructuredFormatter())
            self._logger.addHandler(handler)
            self._logger.setLevel(logging.INFO)

    def _log(self, level: int, event: str, **kwargs: Any):
        record = self._logger.makeRecord(
            self._logger.name, level, "", 0, event, (), None
        )
        record.extra = kwargs
        self._logger.handle(record)

    def info(self, event: str, **kwargs: Any):
        self._log(logging.INFO, event, **kwargs)

    def warning(self, event: str, **kwargs: Any):
        self._log(logging.WARNING, event, **kwargs)

    def error(self, event: str, **kwargs: Any):
        self._log(logging.ERROR, event, **kwargs)

    def debug(self, event: str, **kwargs: Any):
        self._log(logging.DEBUG, event, **kwargs)
