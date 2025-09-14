import logging
import subprocess
import time
from models import MySQL_cluster, format_time
from mysqlconf import CLUSTER_NAMES

logging.basicConfig(level=logging.INFO, filename="x_validation.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

logging.info(f"Running validation script. List of clusters: {', '.join(CLUSTER_NAMES)}")

for cluster_name in CLUSTER_NAMES:
    
    val_start_time = time.time()
    exit_code = 0
    # Если кластер активен, то выключаем его
    cluster_instance = MySQL_cluster(cluster_name)    
    logging.info(f"Working with the '{cluster_name}' cluster'")
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
        if cluster_instance.dump_validation():
            logging.info(f"Taking dump from cluster '{cluster_name}' completed successfully")
    except Exception as e:
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
    val_stop_time = time.time()
    val_duration = val_stop_time - val_start_time
    cluster_instance.val_durations[cluster_name] = format_time(val_duration)
    cluster_instance.exit_codes[cluster_name] = exit_code
    cluster_instance.restor_durations[cluster_name] = format_time(restor_duration)
    cluster_instance.sizes[cluster_name] = cluster_instance.get_size_cluster()

# Формирование файла отчета по всем итерациям (по всем бэкапам)
try:
    if MySQL_cluster.output_stats():
        logging.info(f"Report and monitoring files have been generated")
except Exception as e:
    logging.error(e)

logging.info(f"Script execution completed")


