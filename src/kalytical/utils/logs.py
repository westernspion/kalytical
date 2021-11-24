from typing import Any
import sys
import structlog
from structlog import configure
from structlog.contextvars import (
    bind_contextvars,
    clear_contextvars, merge_contextvars,
    unbind_contextvars
)

import logging

logging.basicConfig(format="%message)s", stream=sys.stderr, level=logging.INFO)


def get_logger(name: str = 'default', log_format: stgr = "text", force: bool = False) -> Any:
    configure(logger_factory=structlog.stdlib.LoggerFactory(),
              wrapper_class=structlog.stdlib.BoundLogger,
              processors=[
        structlog.stdlib.add_logger_name,
        structlog.stdlib.filter_by_level,
        structlog.processors.format_exc_info,
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer()

    ])

    return structlog.get_logger(name)
