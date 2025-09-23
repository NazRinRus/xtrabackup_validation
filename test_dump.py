#!/usr/bin/env python3

import logging
import subprocess
import time
from models import MySQL_cluster, format_time, archive_file
from mysqlconf import CLUSTER_NAMES

logging.basicConfig(level=logging.INFO, filename="x_validation.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

cluster_name = 'crm_prod'
cluster_instance = MySQL_cluster(cluster_name)

val_start_time = time.time()
exit_code = 0
# Снятие дампа
try:
    dbs_tables = cluster_instance.get_tables_in_dbs()
    # базовые параметры для снятия дампа только данных одной таблицы
    parametrs = [
            "--no-create-info",
            "--single-transaction",
            "--set-gtid-purged=OFF"
        ]
    # снятие только дампа схемы
    if cluster_instance.start_dump(dump_filename=f"schema_only_{cluster_name}.dump"): # для тестирования, добавить параметр dump_filename='schema_only_crm_prod.dump'
        logging.info(f"Taking dump schema from cluster '{cluster_name}' completed successfully")
    # циклический вызов метода снятия дампа с таблиц
    for db, tables in dbs_tables.items():
        parametrs.append(db)
        for table in tables:
            parametrs.append(f"--tables {table}")
            if cluster_instance.start_dump(param_list=parametrs, dump_filename=f"{cluster_name}_{db}_{table}.dump"):
                logging.info(f"Taking dump table - '{table}' DB - '{db}' from cluster '{cluster_name}' completed successfully")
                if archive_file(f"{cluster_name}_{db}_{table}.dump"):
                    logging.info(f"Archiving file {cluster_name}_{db}_{table}.dump completed successfully")
            parametrs.pop() # удаляю параметр с таблицей
        parametrs.pop() # удаляю параметр с базой
except subprocess.CalledProcessError as e:
    exit_code = 1
    logging.error(e)

# Блок формирования отчета о текущей итерации
val_stop_time = time.time()
val_duration = val_stop_time - val_start_time
cluster_instance.val_durations[cluster_name] = format_time(val_duration)
logging.info(f"Время снятия дампа с архивированием: {format_time(val_duration)}")
cluster_instance.exit_codes[cluster_name] = exit_code
cluster_instance.sizes[cluster_name] = cluster_instance.get_size_cluster()

