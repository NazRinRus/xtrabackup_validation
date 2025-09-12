import logging
import subprocess
from models import MySQL_cluster
from mysqlconf import CLUSTER_NAMES

logging.basicConfig(level=logging.INFO, filename="py_log.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

for cluster_name in CLUSTER_NAMES:
    # Если кластер активен, то выключаем его
    cluster_instance = MySQL_cluster(cluster_name)    
    logging.info(f"Working with the '{cluster_name}' cluster'")
    if cluster_instance.status_cluster():
        logging.info(f"MySQL service is active")
        try:
            if cluster_instance.stop_cluster():
                logging.info(f"MySQL service stoped. Cluster '{cluster_name}'")
        except subprocess.CalledProcessError as e:
            logging.error(e)
    else: logging.info(f"MySQL service is inactive")
        
    # Очистка директории с данными
    try:
        if cluster_instance.clear_data_dir():
            logging.info(f"Successfully cleared '{cluster_instance.mysql_data_dir}'")
    except subprocess.CalledProcessError as e:
        logging.error(e)

    # Копирование файлов бэкапа в директорию данных
    try:
        if cluster_instance.copy_backup_in_datadir():
            logging.info(f"Copying backup files '{cluster_name}' to data directory '{cluster_instance.mysql_data_dir}' completed successfully ")
    except subprocess.CalledProcessError as e:
        logging.error(e)

    # Восстановление данных
    try:
        if cluster_instance.xtrabackup_restore():
            logging.info(f"Cluster '{cluster_name}' recovery completed successfully")
    except subprocess.CalledProcessError as e:
        logging.error(e)
