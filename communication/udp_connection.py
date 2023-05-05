import os
import abc
import socket
import logging
from typing import Union, Tuple

from communication.message import MessagePackage
from communication.multicast_connection import Connection


class UdpConnection(Connection, metaclass=abc.ABCMeta):
    """
    TCP连接
    """

    def __init__(self, address: str, port: int, **kwargs):
        """
        初始化
        :param address: 连接地址
        :param port: 连接端口号
        :param kwargs: 其他参数
        """
        super().__init__(address, port, **kwargs)

    def _generate_connection(self, family=socket.AF_UNIX, type=socket.SOCK_STREAM) -> socket:
        """
        初始化
        """
        connection = socket.socket(family, type)
        return connection


class UdpServer(UdpConnection):
    """
    UDP服务端
    """

    # 是否正在运行
    __running: bool

    # 连接
    __connection: socket = None

    @property
    def running(self) -> bool:
        """
        是否正在运行
        :return:
        """
        return self.__running

    def __init__(self, address: str, port: int, **kwargs):
        """
        初始化
        """
        self.__running = False
        super().__init__(address, port, **kwargs)

    def _generate_connection(self, family=socket.AF_UNIX, type=socket.SOCK_DGRAM) -> socket:
        """
        监听
        :return:
        """
        self.__connection = super()._generate_connection(socket.AF_INET, socket.SOCK_DGRAM)
        self.__connection.bind((self.address, self.port))
        return self.__connection

    def close(self):
        """
        关闭
        :return:
        """
        if self.__running:
            self.__running = False
            super().close()
            if os.path.exists(self.address):
                os.remove(self.address)
            logging.info((self.name or "UDP服务端") + "关闭")


class UdpClient(UdpConnection):
    """
    TCP客户端
    """

    # 连接
    __connection: socket = None

    def __init__(self, address: str, port: int, **kwargs):
        """
        初始化
        :param address: 服务端地址
        :param port: 服务端端口
        :param kwargs:
        """
        super().__init__(address, port, **kwargs)

    def _generate_connection(self, family=socket.AF_UNIX, type=socket.SOCK_DGRAM) -> socket:
        """
        监听
        :return:
        """
        self.__connection = super()._generate_connection(socket.AF_INET, socket.SOCK_DGRAM)
        return self.__connection

    def send(self, message_package: MessagePackage, destination: Union[Tuple[str, int], str] =None):
        """
        发送消息
        :param message_package: 消息包
        :param destination: 目标地址
        :return:
        """
        super().send(message_package, destination or (self.address, self.port))