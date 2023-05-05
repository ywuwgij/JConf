from threading import Thread

from communication.multicast_connection import MulticastClient
from communication.nodes import SlaveNode
from communication.udp_connection import UdpServer

udp_client_address = "127.0.0.1"
udp_client_port = 10002

def start_slave_node():
    """
    启动从节点
    :return:
    """
    udp_server = UdpServer(udp_client_address, udp_client_port)
    slave_node = SlaveNode(MulticastClient(), udp_server)
    slave_node.start()

if __name__ == "__main__":
    start_slave_node()
