import subprocess
import shlex
import os
import time
from mysqlconf import BACKUP_DIR, MYSQL_DATA_DIR

CONFIG_FILE = './validation.conf'

# Класс обрабатывающий файл конфигурации
class Configuration:
    """
    Класс, парсящий, валидирующий и хранящий конфигурацию скрипта
    """
    config_file = CONFIG_FILE

    @classmethod
    def dir_validate(cls, path_dir):
        """ Метод проверки существования директории """
        if not os.path.exists(path_dir):
            raise FileNotFoundError(f"Directory {path_dir} does not exist")
        if not os.path.isdir(path_dir):
            raise NotADirectoryError(f"{path_dir} is not a directory") 
        return True
    
    @classmethod
    def file_validate(cls, file_path):
        """ Метод проверки существования файла """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} not found")
        return True

    @classmethod
    def parsing_configuration(cls, conf_file_path = './validation.conf'):
        """ Метод парсящий файл конфигурации и сохранящий значения в словарь """
        config_dict = {}
        if cls.file_validate(conf_file_path):
            with open(conf_file_path, 'r', encoding='utf-8') as config_file:
                for line in config_file:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    if '=' in line:
                        key, value = line.split('=', 1)
                        config_dict[key.strip()] = value.strip()
        return config_dict

    @classmethod
    def validation_configuration(cls, config_dict):
        if 'MYSQL_DATA_DIR' in config_dict:
            cls.dir_validate(config_dict['MYSQL_DATA_DIR'])
        else:
            raise NotADirectoryError(f"Missing parameter 'MYSQL_DATA_DIR' in configuration file")
        if 'BACKUP_DIR' in config_dict:
            cls.dir_validate(config_dict['BACKUP_DIR'])
        else:
            raise NotADirectoryError(f"Missing parameter 'BACKUP_DIR' in configuration file")
        return True

    @classmethod
    def get_configuration(cls):
        config_dict = cls.parsing_configuration()
        if cls.validation_configuration(config_dict):
            return config_dict

# Классы описывающие калстера
class MySQL_cluster:
    """
    Класс, описывающий свойства и методы кластера MySQL
    """
    mysql_data_dir = MYSQL_DATA_DIR
    backupdir = BACKUP_DIR
    username = 'mysql'

    @classmethod
    def get_nproc(cls):
        """ Метод получения количества ядер """
        nproc = subprocess.run(
            ["bash", "-c", "nproc"], 
            check=True, 
            capture_output=True, 
            text=True
            )
        if nproc.returncode != 0:
            raise subprocess.CalledProcessError(f"Error executing command nproc: {nproc.stderr}")
        return nproc.stdout.strip()

    @classmethod
    def dir_validate(cls, path_dir):
        """ Метод проверки существования директории """
        if not os.path.exists(path_dir):
            raise FileNotFoundError(f"Directory {path_dir} does not exist")
        if not os.path.isdir(path_dir):
            raise NotADirectoryError(f"{path_dir} is not a directory") 
        return True
    
    @classmethod
    def file_validate(cls, file_path):
        """ Метод проверки существования файла """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} not found")
        return True
    
    @staticmethod
    def stop_cluster():
        """ Метод остановки кластера """
        result_cmd = subprocess.run(
            ["sudo", "systemctl", "stop", "mysql"],
            capture_output=True,
            text=True,
            timeout=60
        )
        time.sleep(2)
        if result_cmd.returncode != 0:
            raise subprocess.CalledProcessError(f"Service mysql stop failed: {result_cmd.stderr}")
        else:
            return True
    
    @staticmethod
    def start_cluster():
        """ Метод запуска кластера """
        result_cmd = subprocess.run(
            ["sudo", "systemctl", "start", "mysql"],
            capture_output=True,
            text=True,
            timeout=60
        )
        time.sleep(2)
        if result_cmd.returncode != 0:
            raise subprocess.CalledProcessError(f"Service mysql start failed: {result_cmd.stderr}")
        else:
            return True

    @classmethod
    def clear_data_dir(cls):
        """ Метод очистки директории с данными """
        # Проверка существования директории
        if cls.dir_validate(cls.mysql_data_dir):
            # Удаление содержимого через rm -rf (только содержимое, не саму папку)
            command = f"rm -rf {shlex.quote(cls.mysql_data_dir)}/* {shlex.quote(cls.mysql_data_dir)}/.* 2>/dev/null || true"
            result = subprocess.run(
                ["sudo", "-u", cls.username, "bash", "-c", command],
                capture_output=True,
                text=True,
                timeout=120
            )
            if result.returncode != 0:
                raise subprocess.CalledProcessError(f"Warning: Some files may not have been deleted: {result.stderr}")
            else:
                return True

    @classmethod
    def extract_uuid_smth(cls):
        """ Метод извлечения значений uuid, smth из файла xtrabackup_galera_info. """
        file_path = cls.mysql_data_dir + '/xtrabackup_galera_info'
        if cls.file_validate(file_path):
            with open(file_path, 'r') as xtrabackup_galera_info:
                content = xtrabackup_galera_info.read().strip()
            uuid = content.split(':')[0]
            smth = content.split(':')[-1]
            file_path = cls.mysql_data_dir + '/grastate.dat'
            content = f"# GALERA saved state\nversion: 2.1\nuuid: {uuid}\nseqno: -1\nsafe_to_bootstrap: 1"
            with open(file_path, 'w') as grastate:
                grastate.write(content)
        return True
    
    def __new__(cls, *args, **kwargs):
        cls.dir_validate(cls.backupdir + '/' + args[0])
        return super().__new__(cls)

    def __init__(self, cluster_name):
        self.cluster_name = cluster_name
    
    def copy_backup_in_datadir(self):
        """
        Метод копирования файлов бэкапа в директорию данных и изменение владельца на mysql
        """
        copy_cmd = f"cp -Rp {shlex.quote(self.backupdir)}/{self.cluster_name}/latest/. {shlex.quote(self.mysql_data_dir)}/"
        result = subprocess.run(["sudo", "bash", "-c", copy_cmd], check=True)
        if result.returncode != 0:
            raise subprocess.CalledProcessError(f"Copy failed: {result.stderr}")
        chown_cmd = f"chown -R {self.username}:{self.username} {shlex.quote(self.mysql_data_dir)}"
        result = subprocess.run(["sudo", "bash", "-c", chown_cmd], check=True)
        if result.returncode != 0:
            raise subprocess.CalledProcessError(f"Owner change error: {result.stderr}")
        return True
        
    def xtrabackup_restore(self):
        """
        Метод восстановления директории данных (когда файлы из бэкапа уже скопированы)
        """
        nproc = self.get_nproc()
        decompress_cmd = f"xtrabackup --parallel={nproc} --decompress --remove-original --target-dir={shlex.quote(self.mysql_data_dir)}"
        result = subprocess.run(["sudo", "-u", self.username, "bash", "-c", decompress_cmd], check=True)
        if result.returncode != 0:
            raise subprocess.CalledProcessError(f"Data decompression error: {result.stderr}")
        restore_cmd = f"xtrabackup --prepare --rebuild-threads={nproc} --target-dir={shlex.quote(self.mysql_data_dir)}"
        result = subprocess.run(["sudo", "-u", self.username, "bash", "-c", restore_cmd], check=True)
        if result.returncode != 0:
            raise subprocess.CalledProcessError(f"Restore failed: {result.stderr}")
        self.extract_uuid_smth()
        chown_cmd = f"chown -R {self.username}:{self.username} {shlex.quote(self.mysql_data_dir)}"
        result = subprocess.run(["sudo", "bash", "-c", chown_cmd], check=True)
        if result.returncode != 0:
            raise subprocess.CalledProcessError(f"Owner change error: {result.stderr}")
        return True

    def get_databases(self):
        """
        Метод получения списка БД, полученный из директорий БД в бэкапе
        """
        path_backup = self.backupdir + '/' + self.cluster_name + '/latest'
        self.dir_validate(path_backup)
        directories = []
        exclude_dirs = ['mysql', 'performance_schema', 'sys']
        with os.scandir(path_backup) as entries:
            for entry in entries:
                if entry.is_dir() and entry.name not in exclude_dirs:
                    directories.append(entry.name)
        return directories
