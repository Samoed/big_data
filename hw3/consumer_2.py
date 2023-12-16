import time
from functools import wraps

from kafka import KafkaConsumer


def backoff(tries: int, sleep: int) -> callable:
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(tries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt < tries - 1:
                        time.sleep(sleep)
                    else:
                        raise e
        return wrapper
    return decorator


@backoff(tries=10, sleep=60)
def message_handler(value) -> None:
    print(value)


def create_consumer():
    print("Connecting to Kafka brokers")
    consumer = KafkaConsumer(
        "itmo2023",
        group_id="itmo_group1",
        bootstrap_servers="localhost:29092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    for message in consumer:
        # send to http get (rest api) to get response
        # save to db message (kafka) + external
        message_handler(message)
        print(message)


if __name__ == "__main__":
    create_consumer()
