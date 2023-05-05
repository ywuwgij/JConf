from communication.multicast_connection import MulticastClient
from communication.nodes import SlaveNode
from communication.udp_connection import UdpServer
from node_config import get_slave_node_config

udp_client_address = "127.0.0.1"
udp_client_port = 10002

def start_slave_node():
    """
    启动从节点
    :return:
    """
    slave_node_config = get_slave_node_config("../../config.json")
    master_address = (slave_node_config.master.ip, slave_node_config.master.port) if slave_node_config.master.ip else None
    udp_server = UdpServer(slave_node_config.config.ip, slave_node_config.config.port)
    slave_node = SlaveNode(MulticastClient(slave_node_config.register.ip, slave_node_config.register.port), udp_server, master_address)
    slave_node.start()

if __name__ == "__main__":
    start_slave_node()
