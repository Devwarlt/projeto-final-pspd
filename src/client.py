#!/usr/bin/env python3

from json import *
from time import *
from redis import *
from typing import *
from hashlib import *
from logging import *
from traceback import *


__CONNECTED_IDS_KEY: str = "connected_ids"
__ENCODING: str = "utf-8"


def get_logger() -> Logger:
    return getLogger(__name__)


def __format_stacktrace(text: str, **kwargs: Dict[str, Any]) -> str:
    message: str = text
    args: Dict[str, Any] = kwargs.pop('args', {})
    if args:
        for key, value in args.items():
            message += f"\n- {key}: {value}"
    return message


def __create_instance_uuid() -> str:
    epoch_tmstmp: str = time().__str__()
    return sha1(epoch_tmstmp.encode(__ENCODING)).hexdigest()


def __get(redis_srv: Redis, key: str) -> Any:
    hashmap: bytes = redis_srv.get(key)
    return loads(hashmap) if hashmap else None


def __set(redis_srv: Redis, key: str, value: Any) -> None:
    hashmap: bytes = dumps(value)
    redis_srv.set(key, hashmap)


def __get_latest_connected_ids(redis_srv: Redis) -> List[str]:
    return __get(redis_srv, __CONNECTED_IDS_KEY)


def __add_new_connection(uuid: str, redis_srv: Redis) -> None:
    connected_ids: List[str] = __get_latest_connected_ids(redis_srv)
    if connected_ids:
        connected_ids.append(uuid)
    else:
        connected_ids = [uuid]

    __set(redis_srv, __CONNECTED_IDS_KEY, connected_ids)


def __remove_exist_connection(uuid: str, redis_srv: Redis) -> None:
    latest_connected_ids: List[str]\
        = __get_latest_connected_ids(redis_srv)
    for connected_id in latest_connected_ids:
        if connected_id == uuid:
            latest_connected_ids.remove(uuid)
            break


def __core(uuid: str, redis_srv: Redis) -> None:
    # code goes here


if __name__ == "__main__":
    log_fmt: Formatter = Formatter(
        '%(asctime)s,%(msecs)-3d - %(levelname)-8s => '
        '%(message)s'
    )
    log_config: Dict[str, Any] = {
        'format': vars(log_fmt).get("_fmt"),
        'datefmt': '%Y-%m-%d %H:%M:%S',
        'level': INFO
    }
    basicConfig(**log_config)

    exit_code: Literal[0, 1] = 0

    redis_cfg: Dict[str, Any] = {
        'host': "localhost",
        'port': 6379,
        'db': 0
    }

    uuid: str = __create_instance_uuid()
    info(f"The UUID of this application is '{uuid}'.")

    redis_srv: Redis = None

    try:
        redis_srv = Redis(**redis_cfg)
        __add_new_connection(uuid, redis_srv)
        __core(uuid, redis_srv)
    except KeyboardInterrupt:
        pass
    except Exception:
        exit_code = 1
        critical(
            __format_stacktrace(
                text="Unexpected process behaviour.",
                args={'Stacktrace': format_exc()}
            )
        )
    finally:
        if redis_srv:
            __remove_exist_connection(uuid, redis_srv)

        info(f"Exit code: {exit_code}")
        sleep(1)
        exit(exit_code)
