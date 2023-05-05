import datetime
import json
import logging
import os.path
from typing import Optional, Dict


class Address:

    # 地址
    __ip: str

    # 端口
    __port: int

    # 名称
    __name: str

    # 描述
    __description: str

    def __init__(self, ip: Optional[str], port:int, name:str, description:str):
        """
        地址
        :param ip: 地址
        :param port: 端口
        :param name: 名称
        :param description: 描述
        """
        self.__ip = ip
        self.__port = port
        self.__name = name
        self.__description = description

    def __str__(self):
        return f"{self.__ip}:{self.__port}  {self.__name}【{self.__description}】"

    @property
    def ip(self) -> str:
        """
        地址
        :return:
        """
        return self.__ip

    @property
    def port(self) -> int:
        """
        端口
        :return:
        """
        return self.__port

    @property
    def name(self) -> str:
        """
        名称
        :return:
        """
        return self.__name

    @property
    def description(self) -> str:
        """
        描述
        :return:
        """
        return self.__description

    @staticmethod
    def from_json_object(json_obj: Dict):
        """
        从json字符串中解析地址
        :param json_obj:
        :return:
        """
        return Address(json_obj["address"], json_obj["port"], json_obj["name"], json_obj["description"])

    def to_json_object(self) -> Dict:
        """
        转换为json对象
        :return:
        """
        return {
            "address": self.__ip,
            "port": self.__port,
            "name": self.__name,
            "description": self.__description
        }


class MasterNodeConfig:

    # 发现服务端
    __discovery: Address = None

    # 配置服务端（为从节点提供配置服务）
    __config: Address = None

    def __init__(self, discovery: Address, config: Address):
        """
        主节点配置
        :param discovery: 发现服务端
        :param config: 配置服务端
        """
        self.__discovery = discovery or Address("224.0.0.1", 20000, "组播发现服务端", "用于接收客户端的注册、心跳，及定期广播配置信息")
        self.__config = config or Address("0.0.0.0", 20001, "配置服务端", "用于接收配置相关服务的配置变更通知")

    @property
    def discovery(self) -> Address:
        """
        发现服务端
        :return:
        """
        return self.__discovery

    @property
    def config(self) -> Address:
        """
        配置服务端
        :return:
        """
        return self.__config

    def __str__(self):
        return f"发现服务端: {self.__discovery}\n配置服务端: {self.__config}"

class SlaveNodeConfig:

    # 注册广播地址
    __register: Address = None

    # 主节点地址
    __master: Address = None

    # 配置服务端（为本地服务提供配置信息）
    __config: Address = None

    def __init__(self, register: Address, master: Address, config: Address):
        """
        从节点配置
        :param register: 注册广播地址
        :param master: 主节点地址
        :param config: 配置服务端
        """
        self.__register = register or Address("224.0.0.1", 20000, "组播注册客户端", "用于向主节点发送握手命令，以及接收主节点的注册反馈")
        self.__master = master or Address(None, 20000, "主节点地址", "用于向主节点发送心跳命令，以及接收主节点的及配置信息")
        self.__config = config or Address("127.0.0.1", 20002, "配置服务端", "用于接收本机服务节点的注册与心跳，以及向服务节点发送配置信息")

    @property
    def register(self) -> Address:
        """
        注册广播地址
        :return:
        """
        return self.__register

    @property
    def master(self) -> Address:
        """
        主节点地址
        :return:
        """
        return self.__master

    @property
    def config(self) -> Address:
        """
        配置服务端
        :return:
        """
        return self.__config

class ServiceNodeConfig:

    # 从节点地址
    __slave: Address = None

    def __init__(self, slave:Address):
        """
        从节点地址
        :param slave:
        """
        self.__slave = slave or Address("127.0.0.1", 20002, "从节点地址", "用于向本机的从节点发送注册与心跳，以及接收从节点的配置信息")

    @property
    def slave(self) -> Address:
        """
        从节点地址
        :return:
        """
        return self.__slave

def get_master_node_config(config_path: str=None) -> MasterNodeConfig:
    """
    获取主节点配置
    :return:
    """
    discovery = None
    config = None
    if config_path and os.path.exists(config_path):
        try:
            with open(config_path, "r") as fs:
                config_data = json.load(fs).get("master", {})
                discovery = Address.from_json_object(config_data.get("discovery", {}))
                config = Address.from_json_object(config_data.get("config", {}))
        except Exception as e:
            logging.error(f"解析主节点配置文件失败: {e}", exc_info=True)
    return MasterNodeConfig(discovery, config)

def get_slave_node_config(config_path: str=None) -> SlaveNodeConfig:
    """
    获取从节点配置
    :return:
    """
    register = None
    master = None
    config = None
    if config_path and os.path.exists(config_path):
        try:
            with open(config_path, "r") as fs:
                config_data = json.load(fs).get("slave", {})
                register = Address.from_json_object(config_data.get("register", {}))
                master = Address.from_json_object(config_data.get("master", {}))
                config = Address.from_json_object(config_data.get("config", {}))
        except Exception as e:
            logging.error(f"解析从节点配置文件失败: {e}", exc_info=True)
    return SlaveNodeConfig(register, master, config)

def save_slave_node_config_master_address(master_ip_address: str, port: int, config_path: str="config.json"):
    """
    保存从节点配置
    :param master_ip_address: 主节点IP地址
    :param port: 主节点端口
    :param config_path:
    :return:
    """
    if config_path and master_ip_address and os.path.exists(config_path):
        try:
            with open(config_path, "r") as fs:
                config_data = json.load(fs)
            tmp_config_path = f'{config_path}~'
            with open(tmp_config_path, "w") as fs:
                slave_master = config_data.get("slave", {}).get("master", {})
                slave_master["address"] = master_ip_address
                slave_master["port"] = port
                json.dump(config_data, fs, indent=4, ensure_ascii=False)
                os.rename(config_path, f"{config_path}.bak.{datetime.datetime.now().strftime('%Y%m%d%H%M%S.%f')}")
                os.rename(tmp_config_path, config_path)
        except Exception as e:
            logging.error(f"保存从节点配置文件失败: {e}", exc_info=True)

def get_service_node_config(config_path: str=None) -> ServiceNodeConfig:
    """
    获取服务节点配置
    :return:
    """
    slave = None
    if config_path and os.path.exists(config_path):
        try:
            with open(config_path, "r") as fs:
                config_data = json.load(fs).get("service", {})
                slave = Address.from_json_object(config_data.get("slave", {}))
        except Exception as e:
            logging.error(f"解析服务节点配置文件失败: {e}", exc_info=True)
    return ServiceNodeConfig(slave)