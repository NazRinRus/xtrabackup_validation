### Скрипт валидации бэкапов кластеров Galera
#### Требования к скрипту валидации бэкапов кластеров PostgreSQL
1. К хосту для проверки бекапов - условно `restore-host` примонтированы директории: `/mnt/tul-backup`, `/mnt/mos-backup`, содержащие бэкапы хостов СУБД - условно `db-host`. Далее в документации директории с бэкапами будут называться `/backup`.
2. Бэкапы хостов MySQL снимаются xtrabackup, директория с бэкапами конкретного хоста называется по имени хоста, соответственно в директории `/backup` будут директории с названиями хостов, в которых будут данные для восстановления.
3. Для проверки возможности восстановления с данных бэкапов, на хосте `restore-host` установлены экземпляры СУБД таких же версий, что и на хостах баз данных. MySQL в основном Percona mysql server версии 5.7, а так-же xtrabackup и утилита qpress из пакета утилит percona.
4. Проверка (валидация) бэкапов осуществляется Python-скриптом:
- 4.1. Бэкапы, предназначенные для восстановления, задаются в ручную в списке `CLUSTER_NAMES` конфигурационного файла `mysqlconf.py`. По элементам списка `CLUSTER_NAMES` будет проходить цикл, в котором будут выполняться следующие действия на хосте `restore_host`:
- 4.1.1. Остановить экземпляр MySQL;
- 4.1.2. Удалить директорию данных остановленного экземпляра СУБД (данные хранятся не в стандартной директории, а в директории `/data/mysql`);
- 4.1.3. Восстановить данные из последнего бэкапа (`/backup/'host_name'/latest`);
- 4.1.4. Запустить экземпляр MySQL;
- 4.1.5. Сделать dump в `/dev/null` , чтобы убедиться, что всё читается.
5. Настроить мониторинг в Zabbix
6. Написать плейбук для Ansible
#### Расположение:
```
/opt/scripts/xtrabackup_validation/

models.py  mysqlconf.py README.md  xtrabackup_validation.py
```
#### Конфигурационный файл mysqlconf.py:
Содержит константы, которые импортируются при выполнении основной логики скрипта
- `MYSQL_DATA_DIR = '/data/mysql'` - Директория с данными, где будет восстанавлен экземпляр БД из фалов бэкапа;
- `BACKUP_DIR = '/test_backup'` - Директория с файлами бэкапов;
- `STATS_DIR = '/var/log/backup_validation'` - Директория, куда пишутся файлы с отчетами, для систем мониторинга;
- `TRUE_DUMP_DIR = '/test_dump'` - Директория, куда, при необходимости, запишутся реальные файлы дампа;
- `CLUSTER_NAMES = ['crm_prod', 'any_test_db']` - Список бэкапов с которыми будет работать скрипт.
#### Файл с описанием классов, дополнительных функций и переменных models.py
Блок импорта библиотек (используются стандартные библиотеки Python, не требующие установки пакетов):
```
import subprocess # для работы с утилитами Linux
import shlex # при работе с путями, валидирует и экранирует
import os # для работы с файлами и директориями
import time # для работы с датой и временем, форматирования
import json # ответы на SQL-запросы, для удобства форматируются в json
from mysqlconf import BACKUP_DIR, MYSQL_DATA_DIR, CLUSTER_NAMES, STATS_DIR, TRUE_DUMP_DIR # переменные конфигурационного файла
```
Функция, преобразующая секунды в формат "чч:мм:сс". Используется в основной логике, для определения времени, затраченного на каждую итерацию.
```
def format_time(seconds):
    """Преобразует секунды в формат чч:мм:сс"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
```
Класс, описывающий свойства и методы кластера MySQL
```
class MySQL_cluster:
    """
    Класс, описывающий свойства и методы кластера MySQL
    """
    # Блок с переменными класса, каждый экземпляр класса видит одни и те же значения, используются для хранения данных - констант,
    # и для аккумулирования статистики каждого экземпляра класса

    # константы из конфигурационного файла
    mysql_data_dir = MYSQL_DATA_DIR
    backupdir = BACKUP_DIR
    stats_dir = STATS_DIR
    true_dump_dir = TRUE_DUMP_DIR
    username = 'mysql'
    # переменные в которые аккумулируется статистика каждого экземпляра
    val_durations = {}
    exit_codes = {}
    restor_durations = {}
    sizes = {}
    discovery = [{'{#STANZA}': cl_name} for cl_name in CLUSTER_NAMES]

    @classmethod
    def output_stats(cls):
        """ 
        Метод записи статистики в файл. По завершении очередной итерации, каждый экземпляр класса добавляет статистику о своей работе в 
        переменные класса. После завершения всех итераций, метод output_stats сохраняет статистику в файл, для систем мониторинга 
        """
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
        """ Метод получения количества ядер. Используется для параллельных процессов. """
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
        """ Метод проверки существования директории. Вызывается перед операциями чтения/записи файлов """
        if not os.path.exists(path_dir):
            raise FileNotFoundError(f"Directory {path_dir} does not exist")
        if not os.path.isdir(path_dir):
            raise NotADirectoryError(f"{path_dir} is not a directory") 
        return True
    
    @classmethod
    def file_validate(cls, file_path):
        """ Метод проверки существования файла. Вызывается перед операциями чтения/записи файлов """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} not found")
        return True
    
    @staticmethod
    def status_cluster():
        """ Метод проверки статуса кластера. Вызывается для проверки пере включением/отключением класстера """
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
        """ 
        Метод извлечения значений uuid, smth из файла xtrabackup_galera_info и помещение в файл grastate.dat. 
        Для корректного запуска кластера, после восстановления данных
        """
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
        Метод получения списка БД из активного кластера, SQL-запросом, клиентской утилитой mysql.
        """
        exclude_db = ['mysql', 'performance_schema', 'sys', 'information_schema']
        command = "mysql --execute='SHOW DATABASES;' --skip-column-names --batch --silent"
        show_databases_cmd = subprocess.run(["sudo", "bash", "-c", command], capture_output=True, text=True, timeout=2)
        databases = [db for db in show_databases_cmd.stdout.strip().split('\n') if db not in exclude_db]
        return sorted(databases)

    @classmethod
    def get_tables_in_dbs(cls):
        """
        Метод получения списка таблиц из каждой БД активного кластера, в виде: {'database: [table_list]'}, 
        SQL-запросом, клиентской утилитой mysql. По данному словарю (dict) будет организован цикл, в котором снимается 
        дамп каждой таблицы.
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
        """
        При создании нового экземпляра класса, проверяется имеется ли в директории с бэкапами экземпляр с именем объекта класса
        """
        cls.dir_validate(os.path.join(cls.backupdir, args[0]))
        return super().__new__(cls)

    def __init__(self, cluster_name):
        """
        При инициализации нового экземпляра класса, ему передается имя экземпляра бэкапа, сохраняется в переменную экземпляра cluster_name
        """
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
        Метод восстановления директории данных (когда файлы из бэкапа уже скопированы), так же вызывает метод класса extract_uuid_smth
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
        Метод получения списка БД из директорий БД в бэкапе. Требуется когда нужно узнать какие БД содержатся в кластере, но он еще не восстановлен
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
        Имя конкретной базы данных следует передавать как элемент списка параметров - [..., 'db_name'], таблицы - 
        параметром [..., '--tables table1 table2 table3'].
        Если передана переменная dump_filename, например dump_filename='schema_only_crm_prod.dump', то дамп запишется в этот файл в директории указанной TRUE_DUMP_DIR конфигурационного файла 
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
        Метод получения размера экземпляра бэкапа. Именно файлы бэкапа, не восстановленная директория с данными
        """
        size_cmd = f"du -sh {os.path.join(self.backupdir, self.cluster_name, 'latest')}"
        size_result = subprocess.run(["sudo", "bash", "-c", size_cmd], capture_output=True, text=True, check=True)
        if size_result.returncode != 0:
            raise subprocess.CalledProcessError(f"Error getting directory size: {size_result.stderr}")
        return size_result.stdout.split()[0].strip()
```
В каждом методе предусмотрены исключения при возникновении ошибки, перехват исключения осуществляется в основной логике, при вызове метода.
#### Файл выполняющий основную логику - xtrabackup_validation.py
Блок импорта библиотек:
- `import logging` - осуществляет логгирование работы скрипта;
- `import subprocess` - в файле основной логики вызов подпроцессов напрямую не осуществляется, но библиотека нужна для отлова исключений;
- `import time` - для замера продолжительности итерации восстановления экземпляра бэкапа;
- `from models import MySQL_cluster, format_time` - импорт основного класса и функции форматирования времени;
- `from mysqlconf import CLUSTER_NAMES` - константа со списком экземпляров бэкапов.

Настройка логгирования, задание формата вывода:
```
logging.basicConfig(level=logging.INFO, filename="x_validation.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")
```
Цикл по списку экземпляров бэкапов, поочередно из списка достаются значения - имя конкретного экземпляра, помещаются в переменную `cluster_name`. Далее каждая итерация работает с этой переменной.
```
for cluster_name in CLUSTER_NAMES:
    # устанавливаю настройки на начало итерации:
    val_start_time = time.time() # засекаем начало итерации
    exit_code = 0 # в начале итерации обнуляем счетчик ошибок

    # Создаю новый экземпляр кластера с именем экземпляра бэкапа текущей итерации. При создании, проверяется наличие директории бэкапа, 
    # соответствующей данному экземпляру класса. Если создание экземпляра не вызвало прерывания, то логгируем о начале работы с этим экземпляром.
    cluster_instance = MySQL_cluster(cluster_name)
    logging.info(f"Working with the '{cluster_name}' cluster'")
    # Если кластер активен, то выключаем его
    if cluster_instance.status_cluster():
        logging.info(f"MySQL service is active")
        try:
            if cluster_instance.stop_cluster():
                logging.info(f"MySQL service stoped. Cluster '{cluster_name}'")
        except subprocess.CalledProcessError as e:
            exit_code = 1
            logging.error(e)
    else: logging.info(f"MySQL service is inactive")
        
    # Очистка директории с данными
    try:
        if cluster_instance.clear_data_dir():
            logging.info(f"Successfully cleared '{cluster_instance.mysql_data_dir}'")
    except subprocess.CalledProcessError as e:
        exit_code = 1
        logging.error(e)

    # Копирование файлов бэкапа в директорию данных
    try:
        if cluster_instance.copy_backup_in_datadir():
            logging.info(f"Copying backup files '{cluster_name}' to data directory '{cluster_instance.mysql_data_dir}' completed successfully ")
    except subprocess.CalledProcessError as e:
        exit_code = 1
        logging.error(e)

    # Восстановление данных
    try:
        start_time = time.time()
        if cluster_instance.xtrabackup_restore():
            end_time = time.time()
            restor_duration = end_time - start_time
            logging.info(f"Cluster '{cluster_name}' recovery completed successfully")
    except subprocess.CalledProcessError as e:
        exit_code = 1
        logging.error(e)

    # Запуск сервиса
    try:
        if cluster_instance.start_cluster():
            logging.info(f"Service 'mysql' - cluster '{cluster_name}' start successful")
    except subprocess.CalledProcessError as e:
        exit_code = 1
        logging.error(e)

    # Снятие дампа
    try:
        dbs_tables = cluster_instance.get_tables_in_dbs()
        # базовые параметры для снятия дампа только данных одной таблицы
        parametrs = [
                "--no-create-info",
                "--single-transaction",
                "--set-gtid-purged=OFF"
            ]
        # снятие дампа только схемы данных
        if cluster_instance.start_dump(): # для реального дампа в файл, добавить параметр dump_filename='file_name'
            logging.info(f"Taking dump schema from cluster '{cluster_name}' completed successfully")
        # циклический вызов метода снятия дампа с таблиц
        for db, tables in dbs_tables.items():
            parametrs.append(db)
            for table in tables:
                parametrs.append(f"--tables {table}")
                if cluster_instance.start_dump(param_list=parametrs):
                    logging.info(f"Taking dump table - '{table}' DB - '{db}' from cluster '{cluster_name}' completed successfully")
                parametrs.pop() # удаляю параметр с таблицей
            parametrs.pop() # удаляю параметр с базой
    except subprocess.CalledProcessError as e:
        exit_code = 1
        logging.error(e)

    # Отключение сервиса
    try:
        if cluster_instance.stop_cluster():
            logging.info(f"Service 'mysql' - cluster '{cluster_name}' stop successful")
    except subprocess.CalledProcessError as e:
        exit_code = 1
        logging.error(e)

    # Очистка директории с данными
    try:
        if cluster_instance.clear_data_dir():
            logging.info(f"Successfully cleared '{cluster_instance.mysql_data_dir}'")
    except subprocess.CalledProcessError as e:
        exit_code = 1
        logging.error(e)

    # Блок формирования отчета о текущей итерации
    val_stop_time = time.time() # засекли время конца итерации
    val_duration = val_stop_time - val_start_time # длительность итерации
    # Текущий экземпляр класса, добавляет свою статистику в переменные класса, что общие для всех экземпляров:
    cluster_instance.val_durations[cluster_name] = format_time(val_duration)
    cluster_instance.exit_codes[cluster_name] = exit_code
    cluster_instance.restor_durations[cluster_name] = format_time(restor_duration)
    cluster_instance.sizes[cluster_name] = cluster_instance.get_size_cluster()

# По окончании всех итераций цикла, формирую файла отчета по всем итерациям (по всем бэкапам)
try:
    if MySQL_cluster.output_stats():
        logging.info(f"Report and monitoring files have been generated")
except Exception as e:
    logging.error(e)

logging.info(f"Script execution completed")
```
