import logging
import subprocess
from models import MySQL_cluster
from mysqlconf import CLUSTER_NAMES

logging.basicConfig(level=logging.INFO, filename="py_log.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

for cluster_name in CLUSTER_NAMES:

    cluster_instance = MySQL_cluster(cluster_name)    

    if cluster_instance.status_cluster():
        logging.info(f"MySQL service is active")
        try:
            if cluster_instance.stop_cluster():
                logging.info(f"MySQL service started. Cluster {cluster_name}")
        except subprocess.CalledProcessError as e:
            logging.error(e)

        
