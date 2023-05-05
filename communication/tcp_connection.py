import abc
import logging
import os
import socket
from communication.multicast_connection import Connection
from communication.connection_info import ConnectionInfo


class TcpConnection(Connection, metaclass=abc.ABCMeta):
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


class TcpServer(TcpConnection):
    """
    TCP服务端
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

    def _generate_connection(self, family=socket.AF_UNIX, type=socket.SOCK_STREAM) -> socket:
        """
        监听
        :return:
        """
        if self.port is not None and self.port != 0:
            self.__connection = super()._generate_connection(socket.AF_INET, socket.SOCK_STREAM)
            self.__connection.bind((self.address, self.port))
        else:
            self.__connection = super()._generate_connection()
            self.__connection.bind(self.address)
        return self.__connection


    def listen(self, backlog: int=100):
        """
        监听
        :param backlog:
        :return:
        """
        if not self.__running:
            self.__running = True
            self.__connection.listen(backlog)
            logging.info((self.name or "TCP服务端") + "开始监听...")
            # 不断接受连接请求，并创建新线程处理连接
            while self.__running:
                # 接受连接请求，并创建新的套接字
                client_socket, client_address = self.__connection.accept()
                if isinstance(client_address, tuple):
                    connection_info = ConnectionInfo(client_socket, client_address[0], client_address[1])
                else:
                    connection_info = ConnectionInfo(client_socket, client_address, 0)
                return connection_info

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
            logging.info((self.name or "TCP服务端") + "关闭")


class TcpClient(TcpConnection):
    """
    TCP客户端
    """

    # 连接
    __connection: socket = None

    # 是否已连接
    __connected: bool = False

    @property
    def is_connected(self) -> bool:
        """
        是否已连接
        :return:
        """
        return self.__connected

    def _generate_connection(self, family=socket.AF_UNIX, type=socket.SOCK_STREAM) -> socket:
        """
        监听
        :return:
        """

        if self.port is not None and self.port != 0:
            self.__connection = super()._generate_connection(socket.AF_INET, socket.SOCK_STREAM)
        else:
            self.__connection = super()._generate_connection()
        return self.__connection

    def connect(self):
        if not self.__connected:
            if self.port is not None and self.port != 0:
                self.__connection.connect((self.address, self.port))
            else:
                self.__connection.connect(self.address)
            self.__connected = True

    def close(self):
        """
        关闭
        :return:
        """
        if self.__connected:
            super().close()
            self.__connected = False
