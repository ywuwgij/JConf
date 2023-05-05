import abc
import logging
import socket
import struct
import uuid
from typing import Union, Tuple

from communication.message import MessagePackage


class Connection(metaclass=abc.ABCMeta):
    """
    组播
    """

    # 组播地址
    __address: str = None

    # 组播端口号
    __port: int = None

    # 组播套接字
    __socket: socket = None

    # 连接名称
    __name: str = None

    @property
    def address(self) -> str:
        """
        获取组播地址
        :return:
        """
        return self.__address

    @property
    def port(self) -> int:
        """
        获取组播端口号
        :return:
        """
        return self.__port

    @property
    def name(self) -> str:
        """
        获取连接名称
        :return:
        """
        return self.__name

    def __init__(self, address: str, port: int, **kwargs):
        """
        初始化
        @param address: 组播地址
        @param port: 组播端口号
        """
        self.__name = kwargs.get("name") or uuid.uuid4().hex
        self.__address = address
        self.__port = port
        self.__socket = self._generate_connection()

    def send(self, message_package: MessagePackage, destination: Union[Tuple[str, int], str] =None):
        """
        发送数据
        :param message_package: 消息包
        :param destination: 目标地址
        :return:
        """
        if destination:
            self.__socket.sendto(message_package.to_json(self.__name), destination)
        else:
            self.__socket.send(message_package.to_json(self.__name))

    def receive(self, size: int=2000) -> Tuple[MessagePackage, Union[str, Tuple[str, int]]]:
        """
        接收数据
        :param size:
        :return:
        """
        data, address = self.__socket.recvfrom(size)
        msg = None
        if data:
            try:
                msg = MessagePackage.from_json(data)
            except Exception as e:
                logging.error(f"数据解析失败：{e}")
        return msg, address

    def close(self):
        """
        关闭
        :return:
        """
        if self.__socket:
            self.__socket.close()

    @abc.abstractmethod
    def _generate_connection(self, family=socket.AF_UNIX, type=socket.SOCK_STREAM) -> socket:
        pass

    def __del__(self):
        """
        析构
        :return:
        """
        self.close()


class MulticastServer(Connection):
    """
    组播器
    """

    def __init__(self, address: str = None, port: int = None, **kwargs):
        address = address or "224.0.0.1"
        port = 10000 if port is None else port
        super().__init__(address, port, **kwargs)

    def _generate_connection(self, family=socket.AF_INET, type=socket.SOCK_DGRAM) -> socket:
        """
        初始化套接字
        :return:
        """
        connection = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        connection.bind(('', self.port))
        # 加入组播组
        connection.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        connection.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton(self.address) + socket.inet_aton('127.0.0.1'))
        return connection


class MulticastClient(Connection):
    """
    组播客户端
    """

    def __init__(self, address: str = None, port: int = None, **kwargs):
        """

        :param address:
        :param port:
        :param kwargs:
        """
        address = address or "224.0.0.1"
        port = 10000 if port is None else port
        super().__init__(address, port, **kwargs)

    def broadcast(self, message_package: MessagePackage):
        """
        广播数据到组播组
        :param message_package: 消息包
        :return:
        """
        super().send(message_package, (self.address, self.port))

    def _generate_connection(self, family=socket.AF_INET, type=socket.SOCK_DGRAM) -> socket:
        # 创建发送socket
        connection = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # 设置数据包跳转数，这里设置为255，表示数据包可以通过255个路由器进行转发
        connection.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 255)
        # 控制组播数据包可以发往发送者所在的主机
        connection.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
        return connection
