import subprocess, threading, queue
import shlex
import os
import time
import json
from mysqlconf import BACKUP_DIR, MYSQL_DATA_DIR, CLUSTER_NAMES, STATS_DIR, TRUE_DUMP_DIR, TRUE_DUMP

# Блок отдельных функций
def format_time(seconds):
    """Преобразует секунды в формат чч:мм:сс"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def archive_file(source_file):
    """ Процедура архивирования файлов """
    source_file_path = os.path.join(TRUE_DUMP_DIR, source_file)
    #source_target_path = os.path.join(TRUE_DUMP_DIR, source_file)
    command = f"tar -czf {TRUE_DUMP_DIR}/{source_file}.tar.gz {source_file_path} --remove-files"
    result = subprocess.run(
        ["sudo", "bash", "-c", command],
        capture_output=True,
        text=True,
        timeout=300
    )
    if result.returncode != 0:
        raise subprocess.CalledProcessError(f"Error during archiving process: {result.stderr}")
    else:
        return True

def tasks_building(dbs_tbls_dict, param_list):
    """ 
    Функция создает список кортежей вида: [(db_name, table_name, [parametrs]),(db_name, table_name, [parametrs]),] 
    """
    tasks = []
    for db, tables in dbs_tbls_dict.items():
        if len(tables) > 0:
            for table in tables:
                if TRUE_DUMP:
                    file_name = f"{db}_{table}.dump"
                    file_path = f"--result-file='{os.path.join(TRUE_DUMP_DIR, file_name)}'"
                else:
                    file_path = ''
                tasks.append(f"mysqldump {' '.join(param_list)} {db} {table} {file_path}")
    return tasks

def start_dump(command_str): 
    dump_result = subprocess.run(["sudo", "bash", "-c", command_str],
        stdout=subprocess.DEVNULL,
        timeout=300  # 5 минут таймаут
    )
    if dump_result.returncode != 0:
        raise subprocess.CalledProcessError(f"Error dumping table")
    else:
        return True

# Классы описывающие калстера
class MySQL_cluster:
    """
    Класс, описывающий свойства и методы кластера MySQL
    """
    mysql_data_dir = MYSQL_DATA_DIR
    backupdir = BACKUP_DIR
    stats_dir = STATS_DIR
    true_dump_dir = TRUE_DUMP_DIR
    username = 'mysql'

    val_durations = {}
    exit_codes = {}
    restor_durations = {}
    sizes = {}
    discovery = [{'{#STANZA}': cl_name} for cl_name in CLUSTER_NAMES]

    @classmethod
    def output_stats(cls):
        """ Метод записи статистики в файл """
        if cls.dir_validate(cls.stats_dir):
            file_path = os.path.join(cls.stats_dir, 'validation_info')
            content = (
                        "EXIT_CODES: " + "; ".join(f"{key}:{value}" for key, value in cls.exit_codes.items()) + "\n"
                        "RESTORE_DURATIONS: " + "; ".join(f"{key}:{value}" for key, value in cls.restor_durations.items()) + "\n"
                        "SIZES: " + "; ".join(f"{key}:{value}" for key, value in cls.sizes.items()) + "\n"
                        "VAL_DURATIONS: " + "; ".join(f"{key}:{value}" for key, value in cls.val_durations.items()) + "\n"
                    )
            with open(file_path, 'w') as validation_info:
                validation_info.write(content)

            file_path = os.path.join(cls.stats_dir, 'stanza_discovery')
            with open(file_path, 'w', encoding='utf-8') as stanza_discovery:
                json.dump(cls.discovery, stanza_discovery, indent=2, ensure_ascii=False)

        return True

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
        return int(nproc.stdout.strip())

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
        """ Метод проверки статуса кластера """
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
        file_path = os.path.join(cls.mysql_data_dir, 'xtrabackup_galera_info')
        if cls.file_validate(file_path):
            with open(file_path, 'r') as xtrabackup_galera_info:
                content = xtrabackup_galera_info.read().strip()
            uuid = content.split(':')[0]
            smth = content.split(':')[-1]
            file_path = os.path.join(cls.mysql_data_dir, 'grastate.dat')
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

    @classmethod
    def get_tables_in_dbs(cls):
        """
        Метод получения списка таблиц из каждой БД активного кластера, в виде: {'database: [table_list]'}
        """
        sql = "SELECT TABLE_SCHEMA, JSON_ARRAYAGG(TABLE_NAME) FROM information_schema.TABLES \
        WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') \
        GROUP BY TABLE_SCHEMA ORDER BY TABLE_SCHEMA;"
        command = f'mysql --execute="{sql}" --skip-column-names --batch --silent'
        show_tables_cmd = subprocess.run(["sudo", "bash", "-c", command], capture_output=True, text=True, timeout=2)
        if show_tables_cmd.returncode != 0:
            raise subprocess.CalledProcessError(f"Failed to retrieve data for tables: {result.stderr}")
        dbs_tbls = {db.strip().split('\t', 1)[0]:db.strip().split('\t', 1)[1].strip('[]"').split('", "') for db in show_tables_cmd.stdout.strip().split('\n')}
        return dbs_tbls

    def __new__(cls, *args, **kwargs):
        cls.dir_validate(os.path.join(cls.backupdir, args[0]))
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
        path_backup = os.path.join(self.backupdir, self.cluster_name, 'latest')
        self.dir_validate(path_backup)
        directories = []
        exclude_dirs = ['mysql', 'performance_schema', 'sys', 'information_schema']
        with os.scandir(path_backup) as entries:
            for entry in entries:
                if entry.is_dir() and entry.name not in exclude_dirs:
                    directories.append(entry.name)
        return sorted(directories)

    def start_dump(self, param_list=None, dump_filename=None):
        """
        Метод запуска процесса снятия дампа. Если список параметров не передан, то снимается только схема данных без самих данных.
        Параметры передаются в виде start_dump(param_list=[...]) или start_dump(param_list=parametrs) 
        Имя конкретной базы данных следует передавать как элемент списка параметра - [..., 'db_name'], таблицы - 
        параметром [..., '--tables table1 table2 table3'].
        Если передана переменная dump_filename, например dump_filename='schema_only_crm_prod.dump', то дамп запишется в этот файл 
        """
        if param_list is None:
            param_list = [
                "--all-databases",
                "--no-data",
                "--routines",
                "--events",
                "--triggers",
                "--single-transaction",
                "--set-gtid-purged=OFF",
                "--skip-add-drop-table"
            ]
        if dump_filename is not None:
            param_list.append(f"--result-file='{os.path.join(self.true_dump_dir, dump_filename)}'") 
        dump_cmd = "mysqldump " + " ".join(param_list) 
        dump_result = subprocess.run(["sudo", "bash", "-c", dump_cmd],
            stdout=subprocess.DEVNULL,
            timeout=300  # 5 минут таймаут
        )
        if dump_result.returncode != 0:
            raise subprocess.CalledProcessError(f"Dump error: {dump_result.stderr}")
        return True

    def get_size_cluster(self):
        """
        Метод получения размера экземпляра бэкапа
        """
        size_cmd = f"du -sh {os.path.join(self.backupdir, self.cluster_name, 'latest')}"
        size_result = subprocess.run(["sudo", "bash", "-c", size_cmd], capture_output=True, text=True, check=True)
        if size_result.returncode != 0:
            raise subprocess.CalledProcessError(f"Error getting directory size: {size_result.stderr}")
        return size_result.stdout.split()[0].strip()


class Worker(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue
        
    def run(self):
        while True:
            try:
                command_str = self.queue.get(True, 1) # результат в виде кортежа
                self.start_dump(command_str[0]) # первый и единственный элемент кортежа
                
                self.queue.task_done()
            except queue.Empty:
                break

    def start_dump(self, command_str): 
        dump_result = subprocess.run(["sudo", "bash", "-c", command_str],
            stdout=subprocess.DEVNULL,
            timeout=300  # 5 минут таймаут
        )
        if dump_result.returncode != 0:
            raise subprocess.CalledProcessError(f"Error dumping table")
        else:
            return True
