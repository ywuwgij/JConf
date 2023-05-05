import uuid
from communication.multicast_connection import MulticastServer
from communication.nodes import MasterNode
from communication.udp_connection import UdpServer
from node_config import get_master_node_config
from settings.repository import LocalNodeClientRepository, LocalSettingRepository
from settings.setting import SettingSection


def start_master_node():
    """
    启动主节点
    :return:
    """
    local_node_client_repository = LocalNodeClientRepository()
    local_node_client_repository.save({"ip": "172.19.90.248", }, "172.19.90.248")
    local_node_client_repository.save({"ip": "10.10.0.231", }, "10.10.0.231")
    local_node_client_repository.save({"ip": "10.10.0.230", }, "10.10.0.230")
    local_setting_repository = LocalSettingRepository()
    local_setting_repository.save(SettingSection("fitting", {}, "fitting", str(uuid.uuid4().hex), "拟合配置项"), "fitting")
    local_setting_repository.save(SettingSection("source", {}, "source", str(uuid.uuid4().hex), "数据源"), "source")
    local_setting_repository.save(SettingSection("map", {}, "map", str(uuid.uuid4().hex), "平面图"), "map")

    master_node_config = get_master_node_config("../../config.json")

    master_node = MasterNode(MulticastServer(master_node_config.discovery.ip, master_node_config.discovery.port),
                             UdpServer(master_node_config.config.ip, master_node_config.config.port),
                             local_node_client_repository, local_setting_repository)
    master_node.start()

if __name__ == "__main__":
    start_master_node()
