import uuid

from communication.nodes import ServiceNode
from communication.tcp_connection import TcpClient
from communication.udp_connection import UdpClient
from master_node import start_master_node
from repository import LocalNodeClientRepository, LocalSettingRepository
from setting import SettingSection

udp_client_address = "127.0.0.1"
udp_client_port = 10002

def start_local_node():
    """
    启动服务节点
    :return:
    """
    udp_client = UdpClient(udp_client_address, udp_client_port)
    service_node = ServiceNode(udp_client)
    service_node.start()

if __name__ == "__main__":
    start_local_node()
