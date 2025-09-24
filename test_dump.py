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
            "--set-gtid-purged=OFF",
            "--skip-triggers",
            "--compact", 
            "--complete-insert"
        ]
    # снятие только дампа схемы
    if cluster_instance.start_dump(dump_filename=f"schema_only_{cluster_name}.dump"): # для тестирования, добавить параметр dump_filename='schema_only_crm_prod.dump'
        logging.info(f"Taking dump schema from cluster '{cluster_name}' completed successfully")
    # циклический вызов метода снятия дампа с таблиц
    for db, tables in dbs_tables.items():
        for table in tables:
            if cluster_instance.start_dump(param_list=parametrs + [db, f"--tables {table}"], dump_filename=f"{cluster_name}_{db}_{table}.dump"):
                logging.info(f"Taking dump table - '{table}' DB - '{db}' from cluster '{cluster_name}' completed successfully")
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


# sudo mysqldump --no-create-info --single-transaction --set-gtid-purged=OFF --skip-triggers --compact --complete-insert crm_prod --tables vtiger_modtracker_detail --result-file='/test_dump/vtiger_modtracker_detail.dump'

# {vtiger_modtracker_detail: 1830.00, vtiger_crmentity: 782.00, vtiger_ticketcf_flag_additional: 1405.00}
