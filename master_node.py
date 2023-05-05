
import uuid

from communication.multicast_connection import MulticastServer
from communication.nodes import MasterNode
from communication.udp_connection import UdpServer
from repository import LocalNodeClientRepository, LocalSettingRepository
from setting import SettingSection

udp_master_address = "0.0.0.0"
udp_master_port = 10001

def start_master_node(local_node_client_repository, local_setting_repository):
    """
    启动主节点
    :param local_node_client_repository:
    :param local_setting_repository:
    :return:
    """
    master_node = MasterNode(MulticastServer(), UdpServer(udp_master_address, udp_master_port), local_node_client_repository, local_setting_repository)
    master_node.start()

if __name__ == "__main__":
    local_node_client_repository = LocalNodeClientRepository()
    local_node_client_repository.save({"ip": "172.19.90.248", }, "172.19.90.248")
    local_node_client_repository.save({"ip": "10.10.0.231", }, "10.10.0.231")
    local_node_client_repository.save({"ip": "10.10.0.230", }, "10.10.0.230")
    local_setting_repository = LocalSettingRepository()
    local_setting_repository.save(SettingSection("fitting", {}, "fitting", str(uuid.uuid4().hex), "拟合配置项"), "fitting")
    local_setting_repository.save(SettingSection("source", {}, "source", str(uuid.uuid4().hex), "数据源"), "source")
    local_setting_repository.save(SettingSection("map", {}, "map", str(uuid.uuid4().hex), "平面图"), "map")

    start_master_node(local_node_client_repository, local_setting_repository)
