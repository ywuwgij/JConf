"""
1、本地组播放地址与端口
2、远程组播放地址与端口
"""
import logging
import threading
import time
from typing import List, Tuple, Union, Optional
from communication.multicast_connection import Connection, MulticastServer, MulticastClient
from communication.connection_info import ConnectionInfo
from communication.message import MessagePackage, MessageType
from communication.udp_connection import UdpServer, UdpClient
from node_config import save_slave_node_config_master_address
from settings.repository import LocalNodeClientRepository, LocalSettingRepository

logging.root.setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')

class ServiceNode:
    """
    服务节点类
    1、发送握手请求（本地UDP)
    2、接收握手成功消息（本地UDP)
    3、接收配置变更信息（本地UDP)
    """

    # 服务节点名称
    __name: str = None

    # UDP客户端
    __udp_client: UdpClient

    # 是否运行
    __running: bool = False

    # 从节点是否连接
    __client_node_connected: bool = False

    def __init__(self, udp_client: UdpClient, **kwargs):
        """
        初始化
        :param udp_client:
        """
        self.__udp_client = udp_client
        self.__name = kwargs.get("name")

    def __receive(self):
        """
        接收消息
        :return:
        """

        while self.__running:
            msg = ''
            try:
                msg, address = self.__udp_client.receive()
                if msg:
                    if msg.message_type == MessageType.HANDSHAKE_RESPONSE:
                        logging.info(f"服务节点接收到从节点{address}的响应握手成功")
                        self.__client_node_connected = True
                    elif msg.message_type == MessageType.HEARTBEAT_RESPONSE:
                        logging.info(f"服务节点接收到从节点{address}的响应心跳成功")
                    elif msg.message_type == MessageType.CONFIGURATION_CHANGE:
                        logging.info(f"服务节点接收到从节点{address}的配置变更通知")
                        # TODO 拉取针对节点的完整的配置信息
                else:
                    logging.info("服务节点接收消息为空")
            except Exception as e:
                logging.error(f"服务节点接收消息{msg}异常{e}" )
            time.sleep(30)

    def __heartbeat(self):
        """
        心跳
        :return:
        """
        while self.__running:
            if self.__client_node_connected:
                try:
                    self.__udp_client.send(MessagePackage(message_type=MessageType.HEARTBEAT_REQUEST, message_content={"name": self.__name}))
                    logging.info(f"服务节点向从节点{self.__udp_client.address}:{self.__udp_client.port}发送心跳")
                except Exception as e:
                    logging.error("心跳异常", e)
                time.sleep(30)
            else:
                client_address = f"{self.__udp_client.address}:{self.__udp_client.port}"
                logging.info(f"服务节点向从节点{client_address}发送握手请求")
                self.__udp_client.send(MessagePackage(message_type=MessageType.HANDSHAKE_REQUEST, message_content={"name": self.__name}))
                time.sleep(10)

    def start(self):
        """
        开始运行
        :return:
        """
        if not self.__running:
            self.__running = True
            threading.Thread(target=self.__receive).start()
            threading.Thread(target=self.__heartbeat).start()

    def close(self):
        """
        关闭
        :return:
        """
        if self.__running:
            self.__running = False
            self.__udp_client.send(MessagePackage(message_type=MessageType.CONNECTION_CLOSE, message_content={"name": self.__name}))
            self.__udp_client.close()

    def __del__(self):
        """
        析构
        :return:
        """
        self.close()

class SlaveNode:

    """
    从节点类

    代理端
    1、通过网络组播广播发送握手请求至主节点
    2、接收配置变更并下发
    2.1、通过网络组播接收的配置变更通知
    2.2、拉取针对节点的完整的配置信息
    2.3、生成并存储配置文件
    2.4、然后通知服务节点

    服务端
    1、接收服务节点的心跳请求（本地UDP)
    2、接收服务节点的配置请求（本地UDP)
    3、服务节点向推送的配置信息（本地UDP)
    2、向服务节点发送配置变更通知（本地UDP)
    """

    # 组播代理端
    __multicast_client: MulticastClient = None

    # 组播服务端
    __multicast_server: MulticastServer = None

    # UDP服务端
    __udp_server: UdpServer = None

    # 主节点连接
    __master_node_address: Union[Tuple[str, int], str] = None

    # 代理端连接
    __client_node_connections: List[ConnectionInfo] = []

    # 代理端连接锁
    __client_node_connections_lock: threading.Lock = threading.Lock()

    # 是否运行
    __running: bool = False

    def __init__(self, multicast_client: MulticastClient, udp_server: UdpServer, master_node_address: Optional[Tuple[str, int]]=None):
        """
        初始化
        :param multicast_client: 组播代理端
        :param udp_server: UDP服务端
        """
        self.__multicast_client = multicast_client or MulticastClient()
        self.__udp_server = udp_server
        self.__master_node_address = master_node_address

    def start(self):
        """
        启动节点

        1、启动组播接收线程
        2、启动组播握手或心跳发送线程
        3、启动本地网络连接接收线程
        4、拉取主节点配置信息
        :return:
        """
        if not self.__running:
            self.__running = True
            # 启动组播代理端
            threading.Thread(target=self.__multicast_receive, args=(self.__multicast_client,)).start()
            # 启动组播握手或心跳发送线程
            threading.Thread(target=self.__handshake_or_heartbeat).start()

            # 启动UDP服务端接收线程
            threading.Thread(target=self.__udp_server_receive).start()

    def __handshake_or_heartbeat(self):
        """
        组播握手或UDP心跳
        :return:
        """
        while self.__running:
            if self.__master_node_address is None:
                # 组播发送握手请求
                logging.info("从节点广播发送握手请求")
                self.__multicast_client.broadcast(MessagePackage(MessageType.HANDSHAKE_REQUEST, None))
                time.sleep(3)
            else:
                # UDP发送心跳请求
                logging.info(f"从节点发送心跳请求至主节点{self.__master_node_address}")
                self.__multicast_client.send(MessagePackage(MessageType.HEARTBEAT_REQUEST, None), self.__master_node_address)
                time.sleep(30)

    def __multicast_receive(self, multicast: Connection):
        """
        接收主组消息

        1、主节点握手成功响应消息
        2、主节点配置变更通知
        :return:
        """
        while self.__running:
            msg, address = multicast.receive()
            # 忽略自己发送的消息
            if msg.sender != multicast.name and (msg.receiver == multicast.name or msg.receiver is None):
                # 只接受一个主节点的握手成功响应消息
                if msg.message_type == MessageType.HANDSHAKE_RESPONSE:
                    if self.__master_node_address is None:
                        # 握手成功响应消息
                        self.__master_node_address = address
                        logging.info(f"从节点成功连接到主节点{self.__master_node_address}")
                        ip_address = self.__master_node_address[0] if isinstance(self.__master_node_address, tuple) else self.__master_node_address
                        port = self.__master_node_address[1] if isinstance(self.__master_node_address, tuple) else 0
                        save_slave_node_config_master_address(ip_address, port)
                elif msg.message_type == MessageType.CONFIGURATION_CHANGE:
                    # 配置变更通知
                    if msg.receiver == multicast.name:
                        logging.info(f"接收到主节点{self.__master_node_address}发送的配置变更通知：" + str(msg))
                    else:
                        logging.info(f"接收到主节点{self.__master_node_address}广播的配置变更通知：" + str(msg))
                    self.__process_configuration_change(msg, address)

    def __process_configuration_change(self, msg: MessagePackage, address: Union[Tuple[str, int], str] ):
        """
        处理配置变更
        1、从主节点拉取完整的配置信息
        2、生成并存储配置文件
        3、通知服务节点
        :param msg: 配置变更消息
        :param address: 主节点地址
        :return:
        """
        # 比较子节点与主节点的配置版本，如果版本不一至则执行下面的操作
        self.__pull_configuration_from_master_node(address)
        # 通知服务节点
        self.__notify_configuration_to_local_node()

    def __pull_configuration_from_master_node(self, address: Union[Tuple[str, int], str]):
        """
        从主节点拉取配置后生成并存储配置文件
        :param address: 主节点地址
        :return:
        """
        # TODO 从主节点拉取配置后生成并存储配置文件

    def __notify_configuration_to_local_node(self):
        """
        通知服务节点
        :return:
        """
        with self.__client_node_connections_lock:
            for connection in self.__client_node_connections:
                self.__send_configuration_to_local_node(connection)

    def __send_configuration_to_local_node(self, connection: ConnectionInfo):
        """
        向服务节点发送配置信息
        :param connection: 连接信息
        :return:
        """
        try:
            self.__udp_server.send(MessagePackage(MessageType.CONFIGURATION_CHANGE, None), connection.socket_address)
        except Exception as e:
            logging.warning("向服务节点发送配置信息失败，原因：{}".format(e))

    def __udp_server_receive(self):
        """
        UDP服务端监听
        :return:
        """
        while self.__running:
            try:
                msg, address = self.__udp_server.receive()
                with self.__client_node_connections_lock:
                    filter_node_connections = [node_connection for node_connection in self.__client_node_connections if node_connection.equal(address)]
                    if len(filter_node_connections) == 0:
                        connection_info = ConnectionInfo.from_address(address)
                        self.__client_node_connections.append(connection_info)
                    else:
                        connection_info = filter_node_connections[0]
                        connection_info.update_heartbeat_ts()

                    if msg.message_type == MessageType.HEARTBEAT_REQUEST:
                        # 心跳请求
                        logging.info(f"从节点接收到服务节点{address}的心跳请求")
                        response = MessagePackage(MessageType.HEARTBEAT_RESPONSE, None)
                        self.__udp_server.send(response, address)
                    elif msg.message_type == MessageType.HANDSHAKE_REQUEST:
                        # 握手请求
                        logging.info(f"从节点接收到服务节点{address}的握手请求")
                        self.__udp_server.send(MessagePackage(MessageType.HANDSHAKE_RESPONSE, None), address)
                    elif msg.message_type == MessageType.CONNECTION_CLOSE:
                        with self.__client_node_connections_lock:
                            self.__client_node_connections.remove(connection_info)
                            logging.info(f"从节点关闭服务节点{address}的连接")
                            break
                    elif msg.message_type == MessageType.CONFIGURATION_REQUEST:
                        # 发送配置信息到服务节点
                        # 解释一下为什么需要请求而不是直接获取
                        # 1、因为有可能配置信息还没有完全准备好，所以需要等待
                        # 2、有可能本地的配置版本与主节点的配置版本相同，所以发送请求可以避免重复读取配置
                        # 3、方便代理端的开发，代理端只需要接收配置变更通知即可
                        self.__send_configuration_to_local_node(connection_info)
                    else:
                        logging.info(f"从节点接收到服务节点{address}的数据：" + str(msg))
            except Exception as ex:
                logging.warning("从节点处理代理端连接异常：" + str(ex))

class MasterNode:

    """
    主节点类

    组播服务端
    1、通过网络组播接收接收子节点的握手请求，并通过收到的组播UDP代理端发送握手成功消息并附加配置拉取地址
    2、主动或被动监听配置变更
    3、通过组播广播配置变更信息

    UDP服务端
    1、接收变更配置通知
    """

    # 组播器
    __multicast_server: MulticastServer

    # UDP服务端
    __udp_server: UdpServer

    # 是否运行
    __running: bool = False

    # 节点代理端仓储
    __node_client_repository: LocalNodeClientRepository

    # 本地配置仓储
    __local_setting_repository: LocalSettingRepository

    # 代理端连接
    __client_connections: List[ConnectionInfo]

    # 代理端连接锁
    __client_connections_lock: threading.Lock()


    def __init__(self, multicast_server: MulticastServer, udp_server: UdpServer,
                 node_client_repository: LocalNodeClientRepository,
                 local_setting_repository: LocalSettingRepository):
        """
        初始化
        :param multicast_server: 组播服务端
        :param udp_server: UDP服务端 (用于接收配置相关服务的配置变更通知)
        :param node_client_repository: 节点代理端仓储
        :param local_setting_repository: 本地配置仓储
        """
        self.__multicast_server = multicast_server
        self.__udp_server = udp_server
        self.__node_client_repository = node_client_repository
        self.__local_setting_repository = local_setting_repository
        self.__client_connections = []
        self.__client_connections_lock = threading.Lock()

    def start(self):
        """
        启动节点

        1、启动组播接收线程
        2、定期广播配置变更信息
        3、启动UDP服务端监听线程
        :return:
        """
        if not self.__running:
            self.__running = True

            # 启动组播接收线程
            threading.Thread(target=self.__multicast_receive).start()

            # 定期广播配置变更信息
            threading.Thread(target=self.__broadcast_configuration_change).start()

            # 启动UDP服务端监听线程
            threading.Thread(target=self.__udp_server_receive).start()

    def __multicast_receive(self):
        """
        接收组播消息

        1、子节点握手请求消息
        1.1、识别此子节点是否是自己的子节点，如果是执行下面的操作
        1.2、是否已经存在此子节点，如果不存在则添加到子节点列表中，更新子节心跳时间
        1.3、回复握手成功消息
        1.4、下发配置版本地址
        :return:
        """
        while self.__running:
            msg, client_node_address = self.__multicast_server.receive()

            client_node_ip_address:str = client_node_address[0]
            port = client_node_address[1]

            # 忽略自己发送的消息
            if msg.sender != self.__multicast_server.name:
                # 识别此子节点是否是自己的子节点，如果是执行下面的操作
                if self.__node_client_repository.get(client_node_ip_address):
                    if msg.message_type == MessageType.HANDSHAKE_REQUEST:
                        logging.info(f"主节点收到从节点{client_node_ip_address}:{port}的握手请求")
                        # 处理子节点心跳
                        self.__hand_client_node_heartbeat(client_node_ip_address, port)
                        # 回复握手成功消息
                        self.__multicast_server.send(MessagePackage(MessageType.HANDSHAKE_RESPONSE, None, msg.sender), client_node_address)
                        # 下发配置版本
                        self.__send_configuration_to_client_node(client_node_address, msg.sender)
                    elif msg.message_type == MessageType.HEARTBEAT_REQUEST:
                        # 处理子节点心跳
                        logging.info(f"主节点收到从节点{client_node_ip_address}:{port}的心跳")
                        self.__hand_client_node_heartbeat(client_node_ip_address, port)
                    elif msg.message_type == MessageType.CONFIGURATION_REQUEST:
                        # 发送配置信息到服务节点
                        logging.info(f"主节点收到从节点{client_node_ip_address}:{port}的配置请求")
                        self.__send_configuration_to_client_node(client_node_address, msg.sender)

    def __hand_client_node_heartbeat(self, client_node_ip_address, port):
        """
        处理子节点心跳
        :param client_node_ip_address: 子节点IP地址
        :param port:
        :return:
        """
        with self.__client_connections_lock:
            # 是否已经存在此子节点，如果不存在则添加到子节点列表中，更新子节心跳时间
            filter_client_connections = [client_connection for client_connection in self.__client_connections if client_connection.equal(client_node_ip_address)]
            if len(filter_client_connections) == 0:
                connection_info = ConnectionInfo.from_address(client_node_ip_address)
                self.__client_connections.append(connection_info)
            else:
                connection_info = filter_client_connections[0]
                connection_info.update_heartbeat_ts()

    def __send_configuration_to_client_node(self, address, receiver: str = None, send_way=MessageType.CONFIGURATION_BROADCAST):
        """
        下发配置版本到子节点
        :param address: 子节点地址
        :param receiver: 接收者
        :param send_way: 发送方式（默认广播）
        :return:
        """
        setting_versions = self.__local_setting_repository.get_module_setting_versions()
        if setting_versions:
            for setting_version in setting_versions:
                # 下发配置版本地址
                self.__multicast_server.send(MessagePackage(send_way, setting_version.to_dict(), receiver), address)

    def __broadcast_configuration_change(self):
        """
        定期广播配置变更信息
        :return:
        """
        while self.__running:
            logging.info("主节点定期广播配置变更信息")
            self.__send_configuration_to_client_node((self.__multicast_server.address, self.__multicast_server.port))
            time.sleep(60*60)
            with self.__client_connections_lock:
                # 移除超时的子节点
                self.__client_connections = [client_connection for client_connection in self.__client_connections if not client_connection.is_expire()]

    def __udp_server_receive(self):
        """
        接收UDP客户端发送的消息
        :return:
        """
        while self.__running:
            try:
                msg, address = self.__udp_server.receive()
                if msg.message_type == MessageType.CONFIGURATION_CHANGE:
                    # TODO 处理并保存配置变更
                    self.__broadcast_configuration_change()
            except Exception as e:
                logging.warning(e)