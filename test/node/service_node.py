from communication.nodes import ServiceNode
from communication.udp_connection import UdpClient
from node_config import get_service_node_config

def start_local_node():
    """
    启动服务节点
    :return:
    """
    service_node_config = get_service_node_config("../../config.json")
    udp_client = UdpClient(service_node_config.slave.ip, service_node_config.slave.port)
    service_node = ServiceNode(udp_client)
    service_node.start()

if __name__ == "__main__":
    start_local_node()
