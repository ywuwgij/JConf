import abc
import os
import pickle
import uuid
from typing import List, Any
from setting import SettingVersionType, SettingVersion


class BaseFileRepository(metaclass=abc.ABCMeta):
    """
    基类仓储
    """

    # 存储目录路径
    __store_dir_path: str

    # 文件扩展名
    __file_extension: str = None

    def __init__(self, object_name: str, **kwargs):
        """
        初始化
        :param kwargs:
        """
        if object_name:
            self.__store_dir_path = os.path.join(kwargs.get("store_dir_path", "/tmp/smart_store"), object_name)
        else:
            raise ValueError("object_name is None")

        if not os.path.exists(self.__store_dir_path):
            os.makedirs(self.__store_dir_path, exist_ok=True)
        self.__file_extension = kwargs.get("file_extension", ".pickle")

    @property
    def get_store_dir_path(self) -> str:
        """
        获取存储目录路径
        :return:
        """
        return self.__store_dir_path

    def __get_file_path(self, _id):
        """
        获取文件路径
        :param _id:
        :return:
        """
        return os.path.join(self.__store_dir_path, _id + self.__file_extension if self.__file_extension else "")

    def get(self, _id):
        """
        获取对象
        :param _id:
        :return:
        """
        obj = None
        file_path = self.__get_file_path(_id)
        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                obj = pickle.load(f)
        return obj

    def save(self, obj, _id):
        """
        保存对象
        :param obj:
        :param _id:
        :return:
        """
        save_file_path = self.__get_file_path(_id)
        if os.path.exists(save_file_path):
            new_save_file_path = f"{save_file_path}.{uuid.uuid4().hex}"
            with open(new_save_file_path, "wb") as f:
                pickle.dump(obj, f)
            os.remove(save_file_path)
            os.rename(new_save_file_path, save_file_path)
        else:
            with open(save_file_path, "wb") as f:
                pickle.dump(obj, f)

    def delete(self, _id):
        """
        删除对象
        :param _id:
        :return:
        """
        file_path = self.__get_file_path(_id)
        if os.path.exists(file_path):
            os.remove(file_path)

    def get_all(self) -> List[Any]:
        """
        获取所有对象
        :return:
        """
        objs = []
        for file_path in os.listdir(self.__store_dir_path):
            if self.__file_extension and not file_path.lower().endswith(self.__file_extension.lower()):
                continue
            with open(os.path.join(self.__store_dir_path, file_path), "rb") as f:
                objs.append(pickle.load(f))
        return objs


class SettingRepository(metaclass=abc.ABCMeta):
    """
    配置仓库
    """

    @abc.abstractmethod
    def get_module_setting_versions(self) -> List[SettingVersion]:
        """
        获得模块配置版本
        @return:
        """
        pass

    @abc.abstractmethod
    def get_section_setting_versions(self, module_name: str) -> List[SettingVersion]:
        """
        获得模块配置版本
        :param module_name: 模块名称
        :return:
        """
        pass


class LocalSettingRepository(SettingRepository, BaseFileRepository):
    """
    本地配置仓库
    """

    def __init__(self, **kwargs):
        """
        初始化
        :param setting_dir_path: 配置文件目录
        :param kwargs:
        """
        BaseFileRepository.__init__(self, "setting", **kwargs)

    def get_module_setting_versions(self) -> List[SettingVersion]:
        """
        获得模块配置版本
        @return:
        """
        setting_sections = self.get_all()
        versions = []
        for setting_section in setting_sections:
            versions.append(SettingVersion(setting_section.module_name, setting_section.version, SettingVersionType.SECTION))
        return versions

    def get_section_setting_versions(self, module_name: str) -> List[SettingVersion]:
        """
        获得模块配置版本
        :param module_name: 模块名称
        :return:
        """
        setting_section = self.get(module_name)
        section_setting_versions = []
        if setting_section:
            section_setting_versions = [SettingVersion(setting_item.name, setting_item.version, SettingVersionType.ITEM) for setting_item in setting_section.get_items()]
        return section_setting_versions

class MasterSettingRepository(LocalSettingRepository):
    """
    主节点端配置仓库
    """

class LocalNodeClientRepository(BaseFileRepository):
    """
    客户节点端仓库
    """

    def __init__(self):
        super().__init__("node_client")