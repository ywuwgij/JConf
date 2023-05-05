import socket
import threading
import time
from typing import Tuple, Union


class ConnectionInfo:
    """
    连接信息
    """

    # 连接
    __connection: socket

    # 地址
    __address: str

    # 端口号
    __port: int

    __lock: threading.Lock = None

    # 连接时间戳
    __connection_ts: float = None

    # 最后心跳时间戳
    __last_heartbeat_ts: float = None

    def __init__(self, connection: socket, address: str, port: int, **kwargs):
        """
        初始化
        @param connection: 连接
        @param address: 地址
        """
        self.__connection = connection
        self.__address = address
        self.__port = port
        self.__lock = kwargs.get("lock") or threading.Lock()
        self.__connection_ts = time.time()

    @property
    def socket(self) -> socket:
        """
        获取连接
        :return:
        """
        return self.__connection

    @property
    def address(self) -> str:
        """
        获取地址
        :return:
        """
        return self.__address

    @property
    def port(self) -> int:
        """
        获取端口号
        :return:
        """
        return self.__port

    @property
    def socket_address(self) -> Tuple[str, int]:
        """
        获取连接地址
        :return:
        """
        return self.__address, self.__port if self.__port > 0 else self.__address

    @property
    def lock(self) -> threading.Lock:
        """
        获取锁
        :return:
        """
        return self.__lock

    @property
    def connection_ts(self) -> float:
        """
        获取连接时间戳
        :return:
        """
        return self.__connection_ts

    @property
    def last_heartbeat_ts(self) -> float:
        """
        获取最后心跳时间戳
        :return:
        """
        return self.__last_heartbeat_ts

    @property
    def full_address(self) -> str:
        """
        获取完整地址
        :return:
        """
        return f"{self.__address}:{self.__port}" if self.__port else self.__address

    def update_heartbeat_ts(self, ts: float=None):
        """
        更新心跳时间戳
        :return:
        """
        self.__last_heartbeat_ts = ts or time.time()

    def is_expire(self, expire: int = 60 * 60) -> bool:
        """
        是否过期
        :param expire: 过期时间（秒），默认1小时
        :return:
        """
        return time.time() - self.__last_heartbeat_ts > expire

    def equal(self, address: Union[str, Tuple]):
        """
        判断是否相等
        :param address:
        :return:
        """
        if isinstance(address, str):
            return self.__address == address
        elif isinstance(address, tuple):
            return self.__address == address[0] and self.__port == address[1]

    @staticmethod
    def from_address(address: Union[str, Tuple]):
        """
        从地址创建连接信息
        :param address:
        :return:
        """
        return ConnectionInfo(None, address[0], address[1]) if isinstance(address, tuple) else ConnectionInfo(None, address, 0)

