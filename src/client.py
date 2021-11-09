#!/usr/bin/env python3

from json import *
from redis import *
from redis.client import *
from string import *
from typing import *
from random import *
from hashlib import *
from logging import *
from traceback import *
from threading import *

from time import (time, sleep)

__CHANNEL_NAME_PATTERN: str = "$uuid CHANNEL"
__CONNECTED_IDS_KEY: str = "connected_ids"
__NETWORK_THREAD_DELAY: float = 2.0
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
    return loads(hashmap.decode(__ENCODING)) if hashmap else None


def __set(redis_srv: Redis, key: str, value: Any) -> None:
    hashmap: bytes = dumps(value)
    redis_srv.set(key, hashmap)


def __get_connected_ids(redis_srv: Redis) -> List[str]:
    return __get(redis_srv, __CONNECTED_IDS_KEY)


def __add_new_connection(uuid: str, redis_srv: Redis) -> None:
    connected_ids: List[str] = __get_connected_ids(redis_srv)
    if connected_ids:
        connected_ids.append(uuid)
    else:
        connected_ids = [uuid]

    __set(redis_srv, __CONNECTED_IDS_KEY, connected_ids)


def __remove_exist_connection(uuid: str, redis_srv: Redis) -> None:
    latest_connected_ids: List[str]\
        = __get_connected_ids(redis_srv)
    for connected_id in latest_connected_ids:
        if connected_id == uuid:
            latest_connected_ids.remove(uuid)
            break

    __set(redis_srv, __CONNECTED_IDS_KEY, latest_connected_ids)


def __get_channel_name(uuid: str) -> str:
    str_template: Template = Template(__CHANNEL_NAME_PATTERN)
    return str_template.substitute(uuid=uuid)


def __handle_incoming_payload(incoming_raw_data: bytes) -> Tuple[str, str]:
    incoming_body: Dict[str, Any] = loads(incoming_raw_data)
    incoming_uuid: str = incoming_body.get('uuid')
    incoming_content: str = incoming_body.get('content')
    return incoming_uuid, incoming_content


def __handle_outgoing_payload(outgoing_uuid: str, outgoing_content: str) -> bytes:
    outgoing_body: Dict[str, Any] = {
        'uuid': outgoing_uuid,
        'content': outgoing_content
    }
    return dumps(outgoing_body).encode(__ENCODING)


def __handle_incoming_messages(redis_srv: Redis, redis_pub: PubSub) -> None:
    while True:
        incoming_payload: Dict[str, Any] = redis_pub.get_message()
        if incoming_payload:
            incoming_data: bytes = incoming_payload.get('data')
            if incoming_data and isinstance(incoming_data, bytes):
                incoming_raw_data: str = incoming_data.decode(__ENCODING)
                incoming_uuid, incoming_content\
                    = __handle_incoming_payload(incoming_raw_data)
                incoming_channel_name: str = __get_channel_name(incoming_uuid)
                info(f"New message received!\n'{incoming_content}'")

                outgoing_uuid: str = uuid
                outgoing_content: str\
                    = f"Hello '{incoming_uuid}'! "\
                    f"It's {outgoing_uuid}! "\
                    f"[{randrange(-1000, 1000).__str__()}] ~ callback"
                outgoing_payload: bytes = __handle_outgoing_payload(
                    outgoing_uuid,
                    outgoing_content
                )
                redis_srv.publish(incoming_channel_name, outgoing_payload)
                continue

        info("There is no message...")
        sleep(__NETWORK_THREAD_DELAY)


def __handle_outgoing_messages(uuid: str, redis_srv: Redis) -> None:
    while True:
        connected_ids: List[str]\
            = __get_connected_ids(redis_srv)
        if connected_ids:
            if connected_ids.__contains__(uuid):
                connected_ids.remove(uuid)
            for connected_id in connected_ids:
                outgoing_channel_name: str = __get_channel_name(connected_id)
                outgoing_uuid: str = uuid
                outgoing_content: str\
                    = f"Hello '{connected_id}'! "\
                    f"It's {outgoing_uuid}! "\
                    f"[{randrange(-1000, 1000).__str__()}] ~ begin"
                outgoing_payload: bytes = __handle_outgoing_payload(
                    outgoing_uuid,
                    outgoing_content
                )
                redis_srv.publish(outgoing_channel_name, outgoing_payload)

        info("There is no connected client...")
        sleep(__NETWORK_THREAD_DELAY)


def __core(uuid: str, redis_srv: Redis) -> None:
    channel_name: str = __get_channel_name(uuid)
    redis_pub: PubSub = redis_srv.pubsub()
    redis_pub.subscribe(channel_name)

    info("Press ANY key to shutdown...")

    incoming_message_handler: Thread = Thread(
        target=__handle_incoming_messages,
        kwargs={
            'redis_srv': redis_srv,
            'redis_pub': redis_pub
        },
        daemon=True
    )
    outgoing_message_handler: Thread = Thread(
        target=__handle_outgoing_messages,
        kwargs={
            'uuid': uuid,
            'redis_srv': redis_srv
        },
        daemon=True
    )

    incoming_message_handler.start()
    outgoing_message_handler.start()

    _ = input()

    incoming_message_handler.join()
    outgoing_message_handler.join()


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
