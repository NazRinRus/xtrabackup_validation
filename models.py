import subprocess
import shlex
import os
import time
from mysqlconf import BACKUP_DIR, MYSQL_DATA_DIR, CLUSTER_NAMES

# Блок отдельных функций
def format_time(seconds):
    """Преобразует секунды в формат чч:мм:сс"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

# Классы описывающие калстера
class MySQL_cluster:
    """
    Класс, описывающий свойства и методы кластера MySQL
    """
    mysql_data_dir = MYSQL_DATA_DIR
    backupdir = BACKUP_DIR
    username = 'mysql'

    val_durations = {}
    exit_codes = {}
    restor_durations = {}
    sizes = {}
    discovery = [{'{#STANZA}': cl_name} for cl_name in CLUSTER_NAMES]

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
    def status_cluster():
        """ Метод остановки кластера """
        result_cmd = subprocess.run(
            ["sudo", "systemctl", "is-active", "mysql"],
            capture_output=True,
            text=True,
            timeout=60
        )
        if result_cmd.returncode not in [0, 3, 4]:
            raise subprocess.CalledProcessError(f"Error getting service status: {result_cmd.stderr}")
        else:
            return True if result_cmd.stdout.strip() == 'active' else False

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
        if cls.dir_validate(cls.mysql_data_dir):
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
    
    @classmethod
    def get_active_databases(cls):
        """
        Метод получения списка БД из активного кластера
        """
        exclude_db = ['mysql', 'performance_schema', 'sys', 'information_schema']
        command = "mysql --execute='SHOW DATABASES;' --skip-column-names --batch --silent"
        show_databases_cmd = subprocess.run(["sudo", "bash", "-c", command], capture_output=True, text=True, timeout=2)
        databases = [db for db in show_databases_cmd.stdout.strip().split('\n') if db not in exclude_db]
        return sorted(databases)

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

    def get_databases_in_backup(self):
        """
        Метод получения списка БД из директорий БД в бэкапе
        """
        path_backup = self.backupdir + '/' + self.cluster_name + '/latest'
        self.dir_validate(path_backup)
        directories = []
        exclude_dirs = ['mysql', 'performance_schema', 'sys', 'information_schema']
        with os.scandir(path_backup) as entries:
            for entry in entries:
                if entry.is_dir() and entry.name not in exclude_dirs:
                    directories.append(entry.name)
        return sorted(directories)

    def start_dump(self, dump_cmd):
        """ Метод запуска процесса снятия дампа"""
        dump_result = subprocess.run(["sudo", "bash", "-c", dump_cmd],
            stdout=subprocess.DEVNULL,
            timeout=300  # 5 минут таймаут на таблицу
        )
        if dump_result.returncode != 0:
            raise subprocess.CalledProcessError(f"Dump error: {dump_result.stderr}")
        return True

    def dump_validation(self):
        """ 
        Метод снятия дампа, если активный кластер развернут из бэкапа текущего экземпляра класса.
        Соответствие кластера проверяется сравнением названий баз данных в активном класстере с кластером экземпляра  
        """
        active_cluster = self.get_active_databases()
        backup_cluster = self.get_databases_in_backup()
        if active_cluster != backup_cluster:
            raise Exception(f"The active cluster is different from the instance cluster - {self.cluster_name}")
        else:
            for db in active_cluster:
                dump_cmd = f"mysqldump --single-transaction --no-data --routines --events --triggers {db} > /dev/null"
                self.start_dump(dump_cmd)
        return True

    def get_size_cluster(self):
        """
        Метод получения размера экземпляра бэкапа
        """
        size_cmd = f"du -sh {self.backupdir}/{self.cluster_name}/latest"
        size_result = subprocess.run(["sudo", "bash", "-c", size_cmd], capture_output=True, text=True, check=True)
        if size_result.returncode != 0:
            raise subprocess.CalledProcessError(f"Error getting directory size: {size_result.stderr}")
        return size_result.stdout.split()[0].strip()
