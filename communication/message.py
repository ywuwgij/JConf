import json
import pickle
from enum import Enum
from typing import Dict


class MessageType(Enum):
    """
    1、消息包的类型 (0:握手请求，1:握手成功，2:配置信息，3:配置变更, 4:心跳)
    """
    # 握手请求 (子节点广播到主节点，服务节点发送到主节点)
    HANDSHAKE_REQUEST = 0
    # 握手成功（主节点回复子节点，子节点回复服务节点）
    HANDSHAKE_RESPONSE = 1
    # 子节点向主节点请求配置信息或服务节点向子节点请求配置信息（请求时可以携带本地址配置版本）
    CONFIGURATION_REQUEST = 2
    # 配置变更（主节点广播到子节点变化的配置或子节点通知服务节点变化的配置，广播时仅发送配置路径与版本）
    CONFIGURATION_CHANGE = 3
    # 配置版本检查（主节点向子节点发送配置版本检查消息）
    CONFIGURATION_BROADCAST = 4
    # 心跳（子节点向主节点广播心跳或服务节点向子节点发送心跳）
    HEARTBEAT_REQUEST = 5
    # 心跳回复（主节点回复子节点心跳或子节点回复服务节点心跳）
    HEARTBEAT_RESPONSE = 6
    # 关闭（服务节点向子节点发送关闭消息）
    CONNECTION_CLOSE = 7

class MessagePackage:
    """
    消息包类
    """

    # 消息类型
    __message_type: MessageType = None

    # 消息内容
    __message_content: Dict = None

    # 消息接收者
    __to: str = None

    # 消息发送者
    __from: str = None

    def __init__(self, message_type: MessageType, message_content: Dict=None, receiver: str=None):
        """
        初始化
        @param message_type: 消息类型
        @param message_content: 消息内容
        """
        self.__message_type = message_type
        self.__message_content = message_content
        self.__to = receiver

    def to_data(self, sender) -> bytes:
        """
        转换为数据
        :return:
        """
        msg = MessagePackage(self.__message_type, self.__message_content, self.__to)
        msg.__from = sender
        return pickle.dumps(msg.to_dict())

    def to_dict(self) -> Dict:
        """
        转换为json
        :return:
        """
        return {
            "message_type": self.__message_type.value,
            "message_content": self.__message_content,
            "to": self.__to,
            "from": self.__from
        }

    def to_json(self, sender=None):
        """
        转换为json
        :return:
        """
        obj = self.to_dict()
        if sender is not None:
            obj["from"] = sender
        return json.dumps(obj).encode("utf-8")

    def __str__(self):
        """
        转换为字符串
        :return:
        """
        return json.dumps(self.to_dict())

    @property
    def message_type(self) -> MessageType:
        """
        获取消息类型
        :return:
        """
        return self.__message_type

    @property
    def message_content(self) -> Dict:
        """
        获取消息内容
        :return:
        """
        return self.__message_content

    @property
    def receiver(self) -> str:
        """
        获取消息接收者
        :return:
        """
        return self.__to

    @property
    def sender(self) -> str:
        """
        获取消息发送者
        :return:
        """
        return self.__from

    @staticmethod
    def from_data(data: bytes) -> "MessagePackage":
        """
        从数据转换
        :param data:
        :return:
        """
        return pickle.loads(data)

    @staticmethod
    def from_dict(data: dict) -> "MessagePackage":
        """
        从json转换
        :param data:
        :return:
        """
        message_package = MessagePackage(data["message_type"], data["message_content"], data["to"])
        message_package.__from = data["from"]
        return message_package

    @staticmethod
    def from_json(json_str: str) -> "MessagePackage":
        """
        从json转换
        :param json_str:
        :return:
        """
        data = json.loads(json_str)
        data["message_type"] = MessageType(data["message_type"])
        return MessagePackage.from_dict(data)
