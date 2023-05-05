import enum
import json
from typing import Any, List, Dict


class SettingVersionType(enum.Enum):
    """
    配置版本类型
    """
    SECTION = 0 # 配置项组
    ITEM = 1 # 配置项

class SettingVersion:
    """
    配置版本
    """

    __name: str = None  # 名称

    __version: str = None  # 版本

    __setting_version_type: SettingVersionType = None  # 类型

    def __init__(self, name: str, version: str, setting_version_type: SettingVersionType):
        """
        初始化
        @param name: 名称
        @param version: 版本
        @param setting_version_type: 类型
        """
        self.__name = name
        self.__version = version
        self.__setting_version_type = setting_version_type

    @property
    def name(self) -> str:
        """
        获取名称
        @return:
        """
        return self.__name

    @property
    def version(self) -> str:
        """
        获取版本
        @return:
        """
        return self.__version

    @property
    def type(self) -> SettingVersionType:
        """
        获取类型
        @return:
        """
        return self.__setting_version_type

    def __str__(self):
        return f"{self.__name} {self.__version} {self.__setting_version_type}"

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        if isinstance(other, SettingVersion):
            return self.__name == other.__name and self.__version == other.__version and self.__setting_version_type == other.__setting_version_type
        else:
            return False

    def to_dict(self) -> Dict[str, Any]:
        """
        转换为字典
        @return:
        """
        return {
            "name": self.__name,
            "version": self.__version,
            "type": self.__setting_version_type.value
        }

    def to_data(self) -> bytes:
        """
        转换为数据
        @return:
        """
        return json.dumps(self.to_dict()).encode()


class SettingSection:
    """
    配置项组
    """
    __name: str = None  # 配置项名称

    __items: Dict[str, "SettingItem"] = None  # 配置项值

    __module_name: str = None  # 系统名称

    __version: str = None  # 版本

    __description: str = None  # 配置项描述

    def __init__(self, name: str, items: Dict[str, "SettingItem"], module_name:str = None, version: str=None, description: str = None):
        """
        初始化
        @param name: 配置项名称
        @param items: 配置项值
        @param description: 配置项描述
        """
        self.__name = name
        self.__items = items
        self.__module_name = module_name
        self.__description = description
        self.__version = version

    @property
    def name(self) -> str:
        """
        获取配置项名称
        @return:
        """
        return self.__name

    @property
    def module_name(self) -> str:
        """
        获取配置项名称
        @return:
        """
        return self.__module_name

    @property
    def description(self) -> str:
        """
        获取配置项描述
        @return:
        """
        return self.__description

    @property
    def version(self) -> str:
        """
        获取配置项组版本
        @return:
        """
        return self.__version


    def get_items(self) -> List["SettingItem"]:
        """
        获取所有配置项
        @return:
        """
        return list(self.__items.values())

    def get_setting(self, item_name: str) -> "SettingItem":
        """
        获取配置项
        @param item_name:
        @return:
        """
        return self.__items.get(item_name)


class SettingItem:
    """
    配置项
    """

    __name: str = None  # 配置项名称

    __value: Any = None  # 配置项值

    __description: str = None  # 配置项描述

    __section: SettingSection = None  # 配置项所属配置项组

    __version: str = None  # 配置项版本

    def __init__(self, name: str, value: Any, section: SettingSection, version: str, description: str = None):
        """
        初始化
        @param name: 配置项名称
        @param value: 配置项值
        @param description: 配置项描述
        @param section: 配置项所属配置项组
        @param version: 配置项版本
        """
        self.__name = name
        self.__value = value
        self.__section = section
        self.__version = version
        self.__description = description

    @property
    def name(self) -> str:
        """
        获取配置项名称
        @return:
        """
        return self.__name

    @property
    def value(self) -> Any:
        """
        获取配置项值
        @return:
        """
        return self.__value

    @property
    def description(self) -> str:
        """
        获取配置项描述
        @return:
        """
        return self.__description

    @property
    def section(self) -> SettingSection:
        """
        获取配置项所属配置项组
        @return:
        """
        return self.__section

    @property
    def version(self) -> str:
        """
        获取配置项版本
        @return:
        """
        return self.__version

    def full_name(self) -> str:
        """
        获取配置项全名
        @return:
        """
        if self.__section is None:
            this_full_name = self.__name
        else:
            this_full_name = self.__section.name + '.' + self.__name
        return this_full_name

    def __str__(self):
        return self.name + "=" + str(self.value)

    def __repr__(self):
        return self.name + "=" + str(self.value)

    def __eq__(self, other):
        if isinstance(other, SettingItem):
            return self.name == other.name and self.value == other.value
        return False

    def __hash__(self):
        return hash(self.name) + hash(self.value)
