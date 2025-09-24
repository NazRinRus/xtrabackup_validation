#!/usr/bin/env python3

import os
import multiprocessing
import logging
import subprocess
import time
from models import MySQL_cluster, format_time, archive_file
from mysqlconf import CLUSTER_NAMES, TRUE_DUMP_DIR

logging.basicConfig(level=logging.INFO, filename="x_validation.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

cluster_name = 'crm_prod'
cluster_instance = MySQL_cluster(cluster_name)

val_start_time = time.time()
exit_code = 0

def building_workers(tables, nproc):
    """ 
    Функция деления таблиц на пачки, для дальнейшего запуска дампа каждой пачки в своем процессе 
    """
    workers = []
    for i in range(len(tables)):
        if len(workers) < ((i % nproc) + 1):
            workers.append([tables[i]])
        else:
            workers[i % nproc].append(tables[i])
    return workers

def start_dump(param_str): 
    dump_result = subprocess.run(["sudo", "bash", "-c", param_str],
        stdout=subprocess.DEVNULL,
        timeout=300  # 5 минут таймаут
    )
    if dump_result.returncode == 0:
        return True
    else:
        return False

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
    nproc = cluster_instance.get_nproc()
    for db, tables in dbs_tables.items():
        workers = []
        for table in tables:
            param_list = parametrs + [db, f"--tables {table}"]
            dump_filename = f"{cluster_name}_{db}_{table}.dump"
            workers.append(f"mysqldump {' '.join(param_list)} --result-file='{os.path.join(TRUE_DUMP_DIR, dump_filename)}' ")
        
        with multiprocessing.Pool(processes=nproc) as pool:
            results_map = pool.map(start_dump, workers)

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
