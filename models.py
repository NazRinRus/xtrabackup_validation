import subprocess
import shlex
import os
import time
from pathlib import Path
from mysqlconf import BACKUP_DIR, MYSQL_DATA_DIR

def clear_data_dir(directory_path, username = 'mysql'):
    """
    Удаление содержимого директории, от имени определенного пользователя, с проверками существования директории и 
    является ли она системной
    """
    try:
        # Проверка существования директории
        if not os.path.exists(directory_path):
            raise FileNotFoundError(f"Directory {directory_path} does not exist")
        
        if not os.path.isdir(directory_path):
            raise NotADirectoryError(f"{directory_path} is not a directory")
        
        # Проверка опасных путей (защита от удаления системных папок)
        dangerous_paths = ['/home', '/etc', '/var', '/usr', '/bin', '/sbin']
        if any(directory_path.startswith(path) for path in dangerous_paths if len(path) > 1) or (directory_path == '/'):
            raise PermissionError(f"Cannot clear system directory: {directory_path}")
        
        # Удаление содержимого через rm -rf (только содержимое, не саму папку)
        command = f"rm -rf {shlex.quote(directory_path)}/* {shlex.quote(directory_path)}/.* 2>/dev/null || true"
        
        result = subprocess.run(
            ["sudo", "-u", username, "bash", "-c", command],
            capture_output=True,
            text=True,
            timeout=120
        )
        
        if result.returncode == 0:
            print(f"Successfully cleared {directory_path} as {username}")
            return True
        else:
            print(f"Warning: Some files may not have been deleted: {result.stderr}")
            return True  # Частичный успех
            
    except Exception as e:
        print(f"Error: {e}")
        return False

def copy_backup_in_datadir(backup_path, data_dir, username = 'mysql'):
    """
    Функция копирования файлов бэкапа в директорию данных
    """
    try:
        copy_cmd = f"cp -Rp {shlex.quote(backup_path)}/. {shlex.quote(data_dir)}/"
        subprocess.run(["sudo", "bash", "-c", copy_cmd], check=True)
        chown_cmd = f"chown -R {username}:{username} {shlex.quote(data_dir)}"
        subprocess.run(["sudo", "bash", "-c", chown_cmd], check=True)
        
        print("Copy completed successfully")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"Copy failed: {e}")
        return False

def extract_uuid_smth(data_dir):
    """
    Функция извлечения значений uuid, smth из файла xtrabackup_galera_info. 
    """
    file_path = data_dir + '/xtrabackup_galera_info'
    try:
        with open(file_path, 'r') as xtrabackup_galera_info:
            content = xtrabackup_galera_info.read().strip()
        
        # Разделяем по двоеточию и берем первую и последнюю часть
        uuid = content.split(':')[0]
        smth = content.split(':')[-1]
        
    except FileNotFoundError:
        print(f"File {file_path} not found")
        return False
    except Exception as e:
        print(f"Error reading file: {e}")
        return False
        
    file_path = data_dir + '/grastate.dat'
    content = f"# GALERA saved state\nversion: 2.1\nuuid: {uuid}\nseqno: -1\nsafe_to_bootstrap: 1"
    try:
        with open(file_path, 'w') as grastate:
            grastate.write(content)

    except Exception as e:
        print(f"Error reading file: {e}")
        return False

def xtrabackup_restore(data_dir, username = 'mysql'):
    """
    Функция восстановления директории данных (файлы из бэкапа уже скопированы)
    """
    try:
        nproc = subprocess.run(
            ["bash", "-c", "nproc"], 
            check=True, 
            capture_output=True, 
            text=True
            ).stdout.strip()
        decompress_cmd = f"xtrabackup --parallel={nproc} --decompress --remove-original --target-dir={shlex.quote(data_dir)}"
        subprocess.run(["sudo", "-u", username, "bash", "-c", decompress_cmd], check=True)
        restore_cmd = f"xtrabackup --prepare --rebuild-threads={nproc} --target-dir={shlex.quote(data_dir)}"
        subprocess.run(["sudo", "-u", username, "bash", "-c", restore_cmd], check=True)
        extract_uuid_smth(data_dir)
        chown_cmd = f"chown -R {username}:{username} {shlex.quote(data_dir)}"
        subprocess.run(["sudo", "bash", "-c", chown_cmd], check=True)
        
        print("Restore completed successfully")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"Restore failed: {e}")
        return False

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
        try:
            result_cmd = subprocess.run(
                ["sudo", "systemctl", "stop", "mysql"],
                capture_output=True,
                text=True,
                timeout=60
            )
            time.sleep(2)
            if result_cmd.returncode == 0:
                print(f"Service mysql stop successful")
                return True
            else:
                print(f"Service mysql stop failed: {result_cmd.stderr}")
                return False
            
        except Exception as e:
            print(f"Error: {e}")
            return False
    
    @staticmethod
    def start_cluster():
        """
        Метод запуска кластера
        """
        try:
            result_cmd = subprocess.run(
                ["sudo", "systemctl", "start", "mysql"],
                capture_output=True,
                text=True,
                timeout=60
            )
            time.sleep(2)
            if result_cmd.returncode == 0:
                print(f"Service mysql start successful")
                return True
            else:
                print(f"Service mysql start failed: {result_cmd.stderr}")
                return False
            
        except Exception as e:
            print(f"Error: {e}")
            return False

    @staticmethod
    def clear_data_dir():
        """
        Метод очистки директории с данными
        """
        try:
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
            if result.returncode == 0:
                print(f"Successfully cleared {MySQL_cluster.mysql_data_dir} as {MySQL_cluster.username}")
                return True
            else:
                print(f"Warning: Some files may not have been deleted: {result.stderr}")
                return True  # Частичный успех 
        except Exception as e:
            print(f"Error: {e}")
            return False

    def copy_backup_in_datadir(self):
        """
        Метод копирования файлов бэкапа в директорию данных
        """
        try:
            copy_cmd = f"cp -Rp {shlex.quote(MySQL_cluster.backupdir)}/{self.cluster_name}/latest/. {shlex.quote(MySQL_cluster.mysql_data_dir)}/"
            subprocess.run(["sudo", "bash", "-c", copy_cmd], check=True)
            chown_cmd = f"chown -R {MySQL_cluster.username}:{MySQL_cluster.username} {shlex.quote(MySQL_cluster.mysql_data_dir)}"
            subprocess.run(["sudo", "bash", "-c", chown_cmd], check=True)
            print("Copy completed successfully")
            return True
        except subprocess.CalledProcessError as e:
            print(f"Copy failed: {e}")
            return False

    @staticmethod
    def extract_uuid_smth():
        """
        Метод извлечения значений uuid, smth из файла xtrabackup_galera_info. 
        """
        file_path = MySQL_cluster.mysql_data_dir + '/xtrabackup_galera_info'
        try:
            with open(file_path, 'r') as xtrabackup_galera_info:
                content = xtrabackup_galera_info.read().strip()
            uuid = content.split(':')[0]
            smth = content.split(':')[-1]
        except FileNotFoundError:
            print(f"File {file_path} not found")
            return False
        except Exception as e:
            print(f"Error reading file: {e}")
            return False
        file_path = MySQL_cluster.mysql_data_dir + '/grastate.dat'
        content = f"# GALERA saved state\nversion: 2.1\nuuid: {uuid}\nseqno: -1\nsafe_to_bootstrap: 1"
        try:
            with open(file_path, 'w') as grastate:
                grastate.write(content)
        except Exception as e:
            print(f"Error reading file: {e}")
            return False

    def xtrabackup_restore(self):
        """
        Метод восстановления директории данных (файлы из бэкапа уже скопированы)
        """
        try:
            nproc = subprocess.run(
                ["bash", "-c", "nproc"], 
                check=True, 
                capture_output=True, 
                text=True
                ).stdout.strip()
            decompress_cmd = f"xtrabackup --parallel={nproc} --decompress --remove-original --target-dir={shlex.quote(MySQL_cluster.mysql_data_dir)}"
            subprocess.run(["sudo", "-u", MySQL_cluster.username, "bash", "-c", decompress_cmd], check=True)
            restore_cmd = f"xtrabackup --prepare --rebuild-threads={nproc} --target-dir={shlex.quote(MySQL_cluster.mysql_data_dir)}"
            subprocess.run(["sudo", "-u", MySQL_cluster.username, "bash", "-c", restore_cmd], check=True)
            self.extract_uuid_smth()
            chown_cmd = f"chown -R {MySQL_cluster.username}:{MySQL_cluster.username} {shlex.quote(MySQL_cluster.mysql_data_dir)}"
            subprocess.run(["sudo", "bash", "-c", chown_cmd], check=True)
            print("Restore completed successfully")
            return True
        except subprocess.CalledProcessError as e:
            print(f"Restore failed: {e}")
            return False
