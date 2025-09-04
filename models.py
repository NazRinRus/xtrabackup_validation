import subprocess
import shlex
import os
import time
from mysqlconf import BACKUP_DIR, MYSQL_DATA_DIR

# Создание кастомных классов исключений
class MysqlStopError(Exception):
    """Ошибка остановки сервиса mysql"""
    pass

class MysqlStartError(Exception):
    """Ошибка остановки сервиса mysql"""
    pass

class ClearDataDirError(Exception):
    """Ошибка очистки директории данных mysql"""
    pass

class CopyBackupInDataDirError(Exception):
    """Ошибка при копировании файлов из бэкапа в директорию данных mysql"""
    pass


# Классы описывающие калстера
class MySQL_cluster:
    """
    Класс, описывающий свойства и методы кластера MySQL
    """
    mysql_data_dir = MYSQL_DATA_DIR
    backupdir = BACKUP_DIR
    username = 'mysql'

    def __init__(self, cluster_name):
        self.cluster_name = cluster_name
    
    @staticmethod
    def stop_cluster():
        """
        Метод остановки кластера
        """
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
        """
        Метод запуска кластера
        """
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

    @staticmethod
    def clear_data_dir():
        """
        Метод очистки директории с данными
        """
        # Проверка существования директории
        if not os.path.exists(MySQL_cluster.mysql_data_dir):
            raise FileNotFoundError(f"Directory {MySQL_cluster.mysql_data_dir} does not exist")
        if not os.path.isdir(MySQL_cluster.mysql_data_dir):
            raise NotADirectoryError(f"{MySQL_cluster.mysql_data_dir} is not a directory")
        # Проверка опасных путей (защита от удаления системных папок)
        dangerous_paths = ['/home', '/etc', '/var', '/usr', '/bin', '/sbin']
        if any(MySQL_cluster.mysql_data_dir.startswith(path) for path in dangerous_paths if len(path) > 1) or (MySQL_cluster.mysql_data_dir == '/'):
            raise PermissionError(f"Cannot clear system directory: {MySQL_cluster.mysql_data_dir}")
        # Удаление содержимого через rm -rf (только содержимое, не саму папку)
        command = f"rm -rf {shlex.quote(MySQL_cluster.mysql_data_dir)}/* {shlex.quote(MySQL_cluster.mysql_data_dir)}/.* 2>/dev/null || true"
        result = subprocess.run(
            ["sudo", "-u", MySQL_cluster.username, "bash", "-c", command],
            capture_output=True,
            text=True,
            timeout=120
        )
        if result.returncode != 0:
            raise subprocess.CalledProcessError(f"Warning: Some files may not have been deleted: {result.stderr}")
        else:
            return True

    def copy_backup_in_datadir(self):
        """
        Метод копирования файлов бэкапа в директорию данных и изменение владельца на mysql
        """
        copy_cmd = f"cp -Rp {shlex.quote(MySQL_cluster.backupdir)}/{self.cluster_name}/latest/. {shlex.quote(MySQL_cluster.mysql_data_dir)}/"
        result = subprocess.run(["sudo", "bash", "-c", copy_cmd], check=True)
        if result.returncode != 0:
            raise subprocess.CalledProcessError(f"Copy failed: {result.stderr}")
        chown_cmd = f"chown -R {MySQL_cluster.username}:{MySQL_cluster.username} {shlex.quote(MySQL_cluster.mysql_data_dir)}"
        result = subprocess.run(["sudo", "bash", "-c", chown_cmd], check=True)
        if result.returncode != 0:
            raise subprocess.CalledProcessError(f"Owner change error: {result.stderr}")
        return True

    @staticmethod
    def extract_uuid_smth():
        """
        Метод извлечения значений uuid, smth из файла xtrabackup_galera_info. 
        """
        file_path = MySQL_cluster.mysql_data_dir + '/xtrabackup_galera_info'
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} not found")
        with open(file_path, 'r') as xtrabackup_galera_info:
            content = xtrabackup_galera_info.read().strip()
        uuid = content.split(':')[0]
        smth = content.split(':')[-1]
        file_path = MySQL_cluster.mysql_data_dir + '/grastate.dat'
        content = f"# GALERA saved state\nversion: 2.1\nuuid: {uuid}\nseqno: -1\nsafe_to_bootstrap: 1"
        with open(file_path, 'w') as grastate:
            grastate.write(content)
        return False

    def xtrabackup_restore(self):
        """
        Метод восстановления директории данных (когда файлы из бэкапа уже скопированы)
        """
        nproc = subprocess.run(
            ["bash", "-c", "nproc"], 
            check=True, 
            capture_output=True, 
            text=True
            ).stdout.strip()
        decompress_cmd = f"xtrabackup --parallel={nproc} --decompress --remove-original --target-dir={shlex.quote(MySQL_cluster.mysql_data_dir)}"
        result = subprocess.run(["sudo", "-u", MySQL_cluster.username, "bash", "-c", decompress_cmd], check=True)
        if result.returncode != 0:
            raise subprocess.CalledProcessError(f"Data decompression error: {result.stderr}")
        restore_cmd = f"xtrabackup --prepare --rebuild-threads={nproc} --target-dir={shlex.quote(MySQL_cluster.mysql_data_dir)}"
        result = subprocess.run(["sudo", "-u", MySQL_cluster.username, "bash", "-c", restore_cmd], check=True)
        if result.returncode != 0:
            raise subprocess.CalledProcessError(f"Restore failed: {result.stderr}")
        self.extract_uuid_smth()
        chown_cmd = f"chown -R {MySQL_cluster.username}:{MySQL_cluster.username} {shlex.quote(MySQL_cluster.mysql_data_dir)}"
        result = subprocess.run(["sudo", "bash", "-c", chown_cmd], check=True)
        if result.returncode != 0:
            raise subprocess.CalledProcessError(f"Owner change error: {result.stderr}")
        return True
