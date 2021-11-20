import logging
from .config import KalyticalConfig

kalytical_config = KalyticalConfig()


def retry(func):
    def inner1(*args, **kwargs):
        retry_max = kalytical_config.operation_retry_count
        attempt = 0

        while attempt < retry_max:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                attempt = attempt + 1
                logging.exception(f"Function is retryable. Attempt={attempt}")
    return inner1
