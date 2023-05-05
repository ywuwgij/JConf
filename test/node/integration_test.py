import os.path
import time
import uuid
from threading import Thread
from communication.multicast_connection import MulticastServer, MulticastClient
from communication.nodes import MasterNode, SlaveNode, ServiceNode
from communication.udp_connection import UdpServer, UdpClient
from repository import LocalNodeClientRepository, LocalSettingRepository
from setting import SettingSection

udp_master_address = "0.0.0.0"
udp_master_port = 10001
udp_client_address = "127.0.0.1"
udp_client_port = 10002

def __start_master_node(local_node_client_repository, local_setting_repository):
    """
    启动主节点
    :param local_node_client_repository:
    :param local_setting_repository:
    :return:
    """
    master_node = MasterNode(MulticastServer(), UdpServer(udp_master_address, udp_master_port), local_node_client_repository, local_setting_repository)
    master_node.start()

def __start_slave_node():
    """
    启动从节点
    :return:
    """
    udp_server = UdpServer(udp_client_address, udp_client_port)
    slave_node = SlaveNode(MulticastClient(), udp_server)
    slave_node.start()

def __start_local_node():
    """
    启动服务节点
    :return:
    """
    udp_client = UdpClient(udp_client_address, udp_client_port)
    service_node = ServiceNode(udp_client)
    service_node.start()

if __name__ == "__main__":
    local_node_client_repository = LocalNodeClientRepository()
    local_node_client_repository.save({"ip": "172.19.90.248", }, "172.19.90.248")
    local_setting_repository = LocalSettingRepository()
    local_setting_repository.save(SettingSection("fitting", {}, "fitting", str(uuid.uuid4().hex), "拟合配置项"), "fitting")
    local_setting_repository.save(SettingSection("source", {}, "source", str(uuid.uuid4().hex), "数据源"), "source")
    local_setting_repository.save(SettingSection("map", {}, "map", str(uuid.uuid4().hex), "平面图"), "map")

    Thread(target=__start_local_node).start()
    time.sleep(3)
    Thread(target=__start_slave_node).start()
    time.sleep(3)
    Thread(target=__start_master_node, args=(local_node_client_repository, local_setting_repository)).start()




